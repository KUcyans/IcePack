#!/bin/bash
#SBATCH --job-name=pmtfication_%j
#SBATCH --partition=icecube
##SBATCH --nodelist=node[194-194] # node[187-191] or node[194-211]
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=40
#SBATCH --mem=60G
#SBATCH --time=48:00:00
#SBATCH --signal=B:USR1@60
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=cyan.jo@proton.me
#SBATCH --output=/dev/null
#SBATCH --error=/dev/null

# Summary mode index: 0=thorsten, 1=geometric featue added, 2=
# SUMMARY_MODE=${SUMMARY_MODE:-0}  # Default to 0 if not set
SUMMARY_MODE=3
PART=21

LOG_DIR="log/"
mkdir -p "${LOG_DIR}"

timestamp=$(date +"%Y%m%d_%H%M%S")
logfile="${LOG_DIR}/[${timestamp}]PMTfy_part${PART}.log"

exec > /dev/null 2> "${logfile}"

echo "Starting job at $(date)"
echo "Running PMTfier.py for part ${PART}"
echo "Summary mode index: ${SUMMARY_MODE}"

python3.9 -u 1.PMTfy.py "${PART}" --summary_mode "${SUMMARY_MODE}"

echo "Job completed at $(date)"
