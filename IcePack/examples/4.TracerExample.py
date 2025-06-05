import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from mpl_toolkits.mplot3d.art3d import Poly3DCollection

from IcePack.Tracer.Tracer import Tracer
from IcePack.Tracer.PMTfiedTracer import PMTfiedTracer
from IcePack.Tracer.PulseMapTracer import PulseMapTracer

# example source directories
source_db_root = (
    "/lustre/hpc/project/icecube/HE_Nu_Aske_Oct2024/sqlite_pulses/"
)
source_pmt_root = "/lustre/hpc/project/icecube/HE_Nu_Aske_Oct2024/PMTfied_filtered_second_round/Snowstorm/CC_CRclean_IntraTravelDistance_250"

# These objects are used to load the data from the source directories
pulse_tracer = PulseMapTracer(source_db_root)
pmtfied_tracer = PMTfiedTracer(source_pmt_root)


# using this with an event number
pmtfied_tracer(event_no=117000300890985)
# or equivalently
pmtfied_tracer(117000300890985)
# will load the event data from the PMTfied data directory


# the tracer objects can be used for example to plot the PMT-fied events
def plot_this_event_PMT(tracer: Tracer, event_no: int, elev=45, azim=120):
    event_df = tracer(event_no)

    fig, ax = plt.subplots(figsize=(18, 13), subplot_kw={"projection": "3d"})

    t1 = event_df["t1"]
    dom_x = event_df["dom_x"]
    dom_y = event_df["dom_y"]
    dom_z = event_df["dom_z"]
    Qtotal = event_df["Qtotal"]
    # # Marker size scaling
    min_marker_size, max_marker_size = 10, 200
    Qmin, Qmax = Qtotal.min(), Qtotal.max()
    marker_sizes = min_marker_size + (Qtotal - Qmin) / (Qmax - Qmin) * (
        max_marker_size - min_marker_size
    )

    # Normalise arrival time for colormap
    t_norm = mcolors.Normalize(vmin=t1.min(), vmax=t1.max())

    # **Plot event scatter points (foreground)**
    sc = ax.scatter(
        dom_x,
        dom_y,
        dom_z,
        c=t1,
        cmap="cool",
        norm=t_norm,
        s=marker_sizes,
        alpha=0.8,
        edgecolors="black",
        linewidth=0.5,
        zorder=2,
    )

    # Colorbar for arrival time
    cbar = plt.colorbar(sc, ax=ax, pad=0.1, shrink=0.75)
    cbar.set_label("Arrival Time (t1)")

    boundary = get_IceCube_boundary()
    z_dust_top = -135
    z_dust_bottom = -215

    verts = []
    for i in range(len(boundary)):
        x1, y1 = boundary[i]
        x2, y2 = boundary[
            (i + 1) % len(boundary)
        ]  # wrap around to first point
        verts.append(
            [
                [x1, y1, z_dust_bottom],
                [x2, y2, z_dust_bottom],
                [x2, y2, z_dust_top],
                [x1, y1, z_dust_top],
            ]
        )

    # Create the top and bottom faces
    top_face = [[x, y, z_dust_top] for x, y in boundary]
    bottom_face = [[x, y, z_dust_bottom] for x, y in boundary]

    poly3d = verts + [top_face, bottom_face]
    boundary_collection = Poly3DCollection(
        poly3d,
        facecolors="grey",
        alpha=0.2,
        zorder=0,
        label="dust layer",
    )  # edgecolors='grey', linewidths=0.5)
    ax.add_collection3d(boundary_collection)

    ax.legend(loc="upper right")

    # **Create a marker size legend**
    size_legend_values = np.linspace(Qmin, Qmax, num=4)
    size_legend_markers = min_marker_size + (size_legend_values - Qmin) / (
        Qmax - Qmin
    ) * (max_marker_size - min_marker_size)

    # Position the legend outside the plot
    legend_ax = fig.add_axes(
        [0.85, 0.05, 0.075, 0.1]
    )  # [left, bottom, width, height]
    legend_ax.set_xticks([])
    legend_ax.set_yticks([])

    for size, q in zip(size_legend_markers, size_legend_values):
        legend_ax.scatter(
            [],
            [],
            s=size,
            edgecolors="black",
            facecolors="none",
            label=f"{q:.2f}",
        )

    legend_ax.legend(loc="center", frameon=False, fontsize=10)

    ax.set_xlabel("X position (m)")
    ax.set_ylabel("Y position (m)")
    ax.set_zlabel("Z position (m)")
    ax.set_title(rf"Event No: {event_no}", fontsize=16)
    ax.invert_xaxis()
    ax.invert_yaxis()
    ax.view_init(elev=elev, azim=azim)

    plt.show()


def get_IceCube_boundary():
    return np.array(
        [
            (-256.1400146484375, -521.0800170898438),
            (-132.8000030517578, -501.45001220703125),
            (-9.13000011444092, -481.739990234375),
            (114.38999938964844, -461.989990234375),
            (237.77999877929688, -442.4200134277344),
            (361.0, -422.8299865722656),
            (405.8299865722656, -306.3800048828125),
            (443.6000061035156, -194.16000366210938),
            (500.42999267578125, -58.45000076293945),
            (544.0700073242188, 55.88999938964844),
            (576.3699951171875, 170.9199981689453),
            (505.2699890136719, 257.8800048828125),
            (429.760009765625, 351.0199890136719),
            (338.44000244140625, 463.7200012207031),
            (224.5800018310547, 432.3500061035156),
            (101.04000091552734, 412.7900085449219),
            (22.11000061035156, 509.5),
            (-101.05999755859375, 490.2200012207031),
            (-224.08999633789062, 470.8599853515625),
            (-347.8800048828125, 451.5199890136719),
            (-392.3800048828125, 334.239990234375),
            (-437.0400085449219, 217.8000030517578),
            (-481.6000061035156, 101.38999938964844),
            (-526.6300048828125, -15.60000038146973),
            (-570.9000244140625, -125.13999938964844),
            (-492.42999267578125, -230.16000366210938),
            (-413.4599914550781, -327.2699890136719),
            (-334.79998779296875, -424.5),
        ]
    )
