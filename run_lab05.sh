#!/bin/bash
# ============================================================
# LRP04 Single PC Multi-Terminal Automation Script
# ============================================================

# ------- CONFIGURATION -------
BINARY="./lab04"            
SOURCE_FILE="RA4.c"
CONFIG_FILE="config.txt"
OUTPUT_CSV="results_local.csv"
LOG_DIR="slave_logs"
SLAVE_START_DELAY=0.5         
RUN_COOLDOWN=2              

# Testing parameters
BASE_PORT=5000
N_VALUES=(4000 8000 16000)
T_VALUES=(2 4 8 16)
RUNS=3

# Colors for readability
GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'; RED='\033[0;31m'
info()    { echo -e "${CYAN}[INFO]${NC}  $*" >&2; }
success() { echo -e "${GREEN}[OK]${NC}    $*" >&2; }

# 1. Setup Environment
mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/*.log
echo "Compiling $SOURCE_FILE..."
gcc -O3 "$SOURCE_FILE" -o "$BINARY" -lpthread -lrt || { echo "Compile failed"; exit 1; }

# 2. Generate config file (Master at Rank 0, then Slaves)
generate_configs() {
    local t=$1
    {
        echo "127.0.0.1 ${BASE_PORT}"
        for (( rank=1; rank<=t; rank++ )); do
            echo "127.0.0.1 $(( BASE_PORT + rank ))"
        done
    } > "$CONFIG_FILE"
}

# 3. Start Slaves in new terminals
start_slaves() {
    local n=$1 t=$2
    for (( rank=1; rank<=t; rank++ )); do
        local port=$(( BASE_PORT + rank ))
        local log_file="${LOG_DIR}/slave_rank${rank}_n${n}.log"
        
        # Opens a new terminal, runs the slave, and logs output to file
        # 'bash -c' is used to handle the redirection inside the new terminal
        gnome-terminal --title="Slave Rank $rank" -- bash -c "$BINARY $n $port 1 > $log_file 2>&1" &
    done
    sleep "$SLAVE_START_DELAY"
}

# 4. Run Master and capture results
run_one() {
    local n=$1 t=$2
    start_slaves "$n" "$t"

    info "Starting master (n=$n, slaves=$t)..." >&2
    local master_out
    master_out=$($BINARY "$n" "$BASE_PORT" 0 2>&1)
    
    # Extract the timing from master output
    local elapsed=$(echo "$master_out" | grep "MASTER time elapsed" | awk '{print $4}')
    
    # Clean up background processes and wait for windows to close
    pkill -f "$BINARY" 2>/dev/null
    echo "$elapsed"
}

main() {
    echo "n,t,Run 1,Run 2,Run 3,Average" > "$OUTPUT_CSV"
    
    for n in "${N_VALUES[@]}"; do
        for t in "${T_VALUES[@]}"; do
            info "--- Testing n=$n, slaves=$t ---"
            generate_configs "$t"
            
            local times=()
            local sum=0
            for (( run=1; run<=RUNS; run++ )); do
                info "Run $run/$RUNS..."
                local res=$(run_one "$n" "$t")
                
                # Ensure we have a number
                if [ -z "$res" ]; then res=0; fi
                
                times+=("$res")
                sum=$(awk "BEGIN {print $sum + $res}")
                sleep 1
            done

            local avg=$(awk "BEGIN {print $sum / $RUNS}")
            echo "${n},${t},${times[0]},${times[1]},${times[2]},${avg}" >> "$OUTPUT_CSV"
            success "Average Time for n=$n, t=$t: $avg seconds"
            sleep "$RUN_COOLDOWN"
        done
    done
    echo "Testing complete. Summary in $OUTPUT_CSV and slave logs in $LOG_DIR."
}

main "$@"