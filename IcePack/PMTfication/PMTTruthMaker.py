import pyarrow as pa
import pyarrow.compute as pc
import sqlite3 as sql
from typing import List
from pyarrow.compute import SetLookupOptions
from IcePack.PMTfication.PMTTruthFromTruth import PMTTruthFromTruth

"""
@cyan.jo
Summary:
Generates labelled truth tables from raw PMTfied outputs.

(1)Aligns PMT-formatted data with original simulation truth.
(2)Handles output structure for learning-ready truth tables.
(3)Supports flexible truth generation per event or shard.
(4)Adds new truth columns calculated from existing truth columns by PMTTruthFromTruth
"""

class PMTTruthMaker:
    def __init__(self, 
                 con_source: sql.Connection, 
                 table_config: dict) -> None:
        self.con_source = con_source
        self.table_config = table_config
        
        self._build_schema()
        self._build_nan_replacement()
        
    def __call__(self, 
                subdirectory_no: int, 
                part_no: int, 
                shard_no: int, 
                event_no_subset: List[int],
                summary_derived_truth_table: pa.Table
                ) -> pa.Table:

        receipt_pa = self._build_receipt_pa(subdirectory_no, part_no, shard_no, event_no_subset)

        # Fetch the TRUTH table using the dedicated query
        truth_table = self._get_pa_shard(
            receipt_pa=receipt_pa,
            event_no_subset=event_no_subset,
            schema_name="truth",
            event_no_column="event_no",
            build_query_func=self._build_truth_query  # Only hardcoded query
        )

        # Fetch all trailing tables dynamically
        trailing_tables = {}
        for name, config in self.table_config.items():
            if name in ("truth", "pulsemap"):
                continue  # skip special tables

            table_name = config["name"]
            event_no_col = config.get("event_no_col", f"{name}_event_no")
            query_builder = self._build_generic_query_builder(name, table_name, event_no_col)

            table = self._get_pa_shard(
                receipt_pa=receipt_pa,
                event_no_subset=event_no_subset,
                schema_name=name,
                event_no_column=event_no_col,
                build_query_func=query_builder
            )
            trailing_tables[name] = (table, event_no_col)

        # Merge truth table with all trailing tables
        merged_table = self._merge_tables(
            truth_table=truth_table,
            trailing_tables=trailing_tables,
            subdirectory_no=subdirectory_no,
            part_no=part_no,
            shard_no=shard_no
        )

        # Join with summary-derived truth table
        merged_table = merged_table.join(summary_derived_truth_table, keys=['event_no'], join_type='inner')

        # Apply secondary derivation
        truth_derived_truth = PMTTruthFromTruth(merged_table)()
        merged_table = merged_table.join(truth_derived_truth, keys=['event_no'], join_type='inner')

        return merged_table


    def _build_receipt_pa(self, subdirectory_no: int, part_no: int, shard_no: int, event_no_subset: List[int]) -> pa.Table:
        receipt_data = {
            'event_no': event_no_subset,
            'subdirectory_no': [subdirectory_no] * len(event_no_subset),
            'part_no': [part_no] * len(event_no_subset),
            'shard_no': [shard_no] * len(event_no_subset),
        }
        return pa.Table.from_pydict(receipt_data)

    def _merge_tables(self, 
                  truth_table: pa.Table, 
                  trailing_tables: dict,
                  subdirectory_no: int, 
                  part_no: int, 
                  shard_no: int) -> pa.Table:
        len_truth = len(truth_table)
        merged_data = {
            'event_no': truth_table['event_no'],
            'subdirectory_no': pa.array([subdirectory_no] * len_truth),
            'part_no': pa.array([part_no] * len_truth),
            'shard_no': pa.array([shard_no] * len_truth),
            **{col: truth_table[col] for col in truth_table.column_names if col != 'event_no'},
        }

        for name, (table, event_no_col) in trailing_tables.items():
            schema = self._SCHEMAS[name]
            if len(table) == len_truth:
                for col in table.column_names:
                    if col != event_no_col:
                        merged_data[col] = table[col]
            else:
                dummy_cols = self._build_empty_table_with_defaults(
                    schema, len_truth, exclude_fields=[event_no_col]
                )
                merged_data.update(dummy_cols)

        return pa.Table.from_pydict(merged_data, schema=self._MERGED_SCHEMA)


    def _filter_rows(self, table: pa.Table, receipt_pa: pa.Table, event_no_column: str) -> pa.Table:
        event_no_column_truth_list = table[event_no_column].to_pylist()
        event_no_column_receipt_list = receipt_pa['event_no'].to_pylist()

        if not event_no_column_truth_list or not event_no_column_receipt_list:
            return pa.Table.from_pydict({field.name: [] for field in self._MERGED_SCHEMA}, schema=self._MERGED_SCHEMA)

        lookup_options = SetLookupOptions(value_set=pa.array(event_no_column_receipt_list))
        filtered_rows = pc.is_in(pa.array(event_no_column_truth_list), options=lookup_options)
        return table.filter(filtered_rows)


    # --------- TABLE SHARD GETTERS ---------
    def _get_pa_shard(self, 
                    receipt_pa: pa.Table, 
                    event_no_subset: List[int], 
                    schema_name: str, 
                    event_no_column: str, 
                    build_query_func: callable) -> pa.Table:
        """
        Generic function to retrieve a PyArrow table shard from the database.
        Parameters:
            - receipt_pa: PyArrow Table containing receipt data.
            - event_no_subset: List of event numbers to filter by.
            - schema_name: The name of the schema (e.g., 'TRUTH', 'GNLabel').
            - event_no_column: The event number column name in the resulting table.
            - build_query_func: The query-building function specific to the schema.

        Returns:
            - A filtered PyArrow table shard.
        """
        query = build_query_func(event_no_subset)
        rows, columns = self._execute_query(query)

        schema = self._SCHEMAS[schema_name]
        nan_replacement = self._nan_replacements[schema_name]


        if not rows:
            # Return an empty table with correct schema
            return pa.Table.from_pydict({field.name: [] for field in schema}, schema=schema)

        table = (
            self._create_truth_pa_table(rows, columns)
            if schema_name == "truth"
            else self._create_trailing_pa_table(rows, columns, schema, nan_replacement)
        )
        return self._filter_rows(table, receipt_pa, event_no_column)


    # --------- TABLE BUILDERS ---------
    def _create_truth_pa_table(self, rows: List[tuple], columns: List[str]) -> pa.Table:
        if not rows:
            schema = self._SCHEMAS["truth"]
            return pa.Table.from_pydict({field.name: [] for field in schema}, schema=schema)

        truth_data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        truth_data['offset'] = pc.cumulative_sum(pa.array(truth_data['N_doms']))
        
        schema = self._SCHEMAS["truth"]
        table = pa.Table.from_pydict(truth_data, schema=schema)

        nan_replacement = self._nan_replacements["truth"]
        for column, replacement in nan_replacement.items():
            if column in table.column_names:
                filled_column = pc.fill_null(table[column], replacement)
                replaced_column = pc.if_else(
                    pc.is_nan(filled_column),
                    pa.scalar(replacement, type=filled_column.type),
                    filled_column
                )
                table = table.set_column(table.schema.get_field_index(column), column, replaced_column)

        return table

    def _create_trailing_pa_table(self, rows: List[tuple], columns: List[str], schema: pa.Schema, nan_replacement: dict) -> pa.Table:
        if not rows:
            return pa.Table.from_pydict({field.name: [] for field in schema}, schema=schema)
        
        data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        table = pa.Table.from_pydict(data, schema=schema)
        
        for column, replacement in nan_replacement.items():
            if column in table.column_names:
                filled_column = pc.fill_null(table[column], replacement)
                replaced_column = pc.if_else(
                    pc.is_nan(filled_column),
                    pa.scalar(replacement, type=filled_column.type),
                    filled_column
                )
                table = table.set_column(table.schema.get_field_index(column), column, replaced_column)
                
        return table

    
    # --------- dynamic SCHEMA BUILDERS ---------
    def _build_schema(self) -> None:
        self._SCHEMAS = {}

        for name, table_info in self.table_config.items():
            if name == "pulsemap": 
                continue

            table_name = table_info["name"]
            original_schema = self.infer_schema_from_sql(self.con_source, table_name)

            if name == "truth":
                self._SCHEMAS[name] = original_schema
            else:
                # Rename 'event_no' to '<name>_event_no'
                renamed_fields = []
                for field in original_schema:
                    if field.name == "event_no":
                        renamed_fields.append(pa.field(f"{name}_event_no", field.type))
                    else:
                        renamed_fields.append(field)
                self._SCHEMAS[name] = pa.schema(renamed_fields)

        # Build merged schema dynamically
        def exclude_event_no_suffix(field):
            return not field.name.endswith("_event_no")

        self._MERGED_SCHEMA = pa.schema(
            list(self._SCHEMAS["truth"]) +
            [
                f for name in self._SCHEMAS
                if name not in ("truth", "pulsemap")
                for f in self._SCHEMAS[name]
                if exclude_event_no_suffix(f)
            ]
        )

    
    def infer_schema_from_sql(self, conn: sql.Connection, 
                              table_name: str) -> pa.Schema:
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        rows = cursor.fetchall()  # each row: (cid, name, type, notnull, dflt_value, pk)

        fields = []
        for _, name, sql_type, *_ in rows:
            arrow_type = self.sql_to_arrow_type(sql_type)
            final_type = arrow_type
            fields.append(pa.field(name, final_type))
        print(f"[INFO] Inferred schema for {table_name}: {fields}")
        return pa.schema(fields)

    def sql_to_arrow_type(self, sql_type: str) -> pa.DataType:
        sql_type = sql_type.lower()
        if "int" in sql_type:
            dtype = pa.int32()
        elif "bigint" in sql_type:
            dtype = pa.int64()
        elif "float" in sql_type or "real" in sql_type or "double" in sql_type:
            dtype = pa.float32()
        elif "text" in sql_type or "char" in sql_type:
            dtype = pa.string()
        else:
            dtype = pa.string()  # fallback
        return dtype

    # --------- NAN REPLACEMENTS ---------
    def _build_nan_replacement(self) -> None:
        print("[INFO] Building NaN replacement values for all table schemas...")
        """
        Builds a dictionary of NaN replacement values for each table schema.
        Relies only on table_config contents (e.g., 'defaults' and 'global_default').
        """
        self._nan_replacements = {}

        for name, config in self.table_config.items():
            # Allow truth table to be processed now
            schema = self._SCHEMAS.get(name)
            if not schema:
                continue

            per_column_defaults = config.get("defaults", {})
            global_default = config.get("global_default", None)

            replacement_map = {}
            for field in schema:
                if field.name.endswith("_event_no"):
                    continue  # skip join keys
                if field.name in per_column_defaults:
                    replacement = per_column_defaults[field.name]
                elif global_default is not None:
                    replacement = global_default
                elif pa.types.is_floating(field.type):
                    replacement = float('nan')
                else:
                    replacement = -1
                replacement_map[field.name] = replacement

            self._nan_replacements[name] = replacement_map



    # --------- QUERY BUILDERS ---------
    def _build_truth_query(self, event_no_subset: List[int]) -> str:
        if not event_no_subset:
            raise ValueError("event_no_subset is empty. Cannot construct a valid SQL query.")

        truth_table_name = self.table_config["truth"]["name"]
        pulsemap_table_name = self.table_config["pulsemap"]["name"]
        excluded_columns = self.table_config["truth"].get("excluded_columns", [])

        columns = [
            field.name for field in self._SCHEMAS["truth"]
            if field.name not in excluded_columns and field.name != 'event_no'
        ]

        select_columns = ['t.event_no'] + [f"t.{col}" for col in columns]

        select_clause = ",\n                ".join(
            select_columns + ["COUNT(DISTINCT s.string || '-' || s.dom_number) AS N_doms"]
        )

        event_filter = ','.join(map(str, event_no_subset))

        query = f"""
            SELECT 
                    {select_clause}
            FROM {truth_table_name} t
            JOIN {pulsemap_table_name} s ON t.event_no = s.event_no
            WHERE t.event_no IN ({event_filter})
            GROUP BY t.event_no
        """
        
        print(f"\n[DEBUG] Final TRUTH SQL query:\n{query.strip()}\n")
        return query

    def _build_generic_query_builder(self, schema_name: str, table_name: str, alias_prefix: str = None):

        def query_builder(event_no_subset: List[int]) -> str:
            event_filter = ','.join(map(str, event_no_subset))
            schema = self._SCHEMAS[schema_name]
            columns = [f.name for f in schema]

            select_columns = []
            for col in columns:
                if col.endswith("_event_no"):
                    original_col = "event_no"
                    select_columns.append(f"{original_col} AS {col}")
                else:
                    select_columns.append(col)

            if not select_columns:
                raise RuntimeError(f"[ERROR] No columns to SELECT from table {table_name}. Schema: {columns}")

            select_clause = ", ".join(select_columns)

            query = f"""
                SELECT {select_clause}
                FROM {table_name}
                WHERE event_no IN ({event_filter})
            """
            return query

        return query_builder


    def _execute_query(self, query: str) -> (List[tuple], List[str]):
        cursor = self.con_source.cursor()

        # LOG BEFORE EXECUTING to make sure it's visible even if broken
        print(f"\n[DEBUG] About to execute SQL query:\n{query.strip()}", flush=True)

        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return rows, columns
        except sql.OperationalError as e:
            print(f"\n[ERROR] SQLite error during query:\n{query.strip()}", flush=True)
            print(f"[EXCEPTION] {e}", flush=True)
            raise



    def _build_empty_table_with_defaults(self, schema: pa.Schema, length: int, exclude_fields: List[str]) -> dict:
        defaults = {}
        for field in schema:
            if field.name in exclude_fields:
                continue
            for name, repl_map in self._nan_replacements.items():
                if field.name in repl_map:
                    replacement = repl_map[field.name]
                    break
            else:
                replacement = float('nan') if pa.types.is_floating(field.type) else -1
            defaults[field.name] = pa.array([replacement] * length, type=field.type)
        return defaults
