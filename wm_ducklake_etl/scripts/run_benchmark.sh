#!/usr/bin/env bash
#
# Main benchmark runner for the TPC-DS ETL benchmark.
#
# Usage:
#   ./scripts/run_benchmark.sh <COMPETITOR> [MODE]
#
# COMPETITOR: windmill | airflow | dagster | dbt | snowflake
# MODE:       docker (default) | k8s
#
# Examples:
#   ./scripts/run_benchmark.sh windmill
#   ./scripts/run_benchmark.sh airflow k8s
#   ./scripts/run_benchmark.sh dbt docker

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$ROOT_DIR/results"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"

COMPETITOR="${1:?Usage: $0 <windmill|airflow|dagster|dbt|snowflake> [docker|k8s]}"
MODE="${2:-docker}"

# Windmill defaults
WM_BASE_URL="${WM_BASE_URL:-http://localhost:8000}"
WM_TOKEN="${WM_TOKEN:-}"
WM_WORKSPACE="${WM_WORKSPACE:-main}"
WM_FLOW_PATH="${WM_FLOW_PATH:-f/tpcds_etl/tpcds_etl}"

# Airflow defaults
AIRFLOW_BASE_URL="${AIRFLOW_BASE_URL:-http://localhost:8080}"
AIRFLOW_USER="${AIRFLOW_USER:-airflow}"
AIRFLOW_PASSWORD="${AIRFLOW_PASSWORD:-airflow}"
AIRFLOW_DAG_ID="${AIRFLOW_DAG_ID:-tpcds_etl}"

# Dagster defaults
DAGSTER_BASE_URL="${DAGSTER_BASE_URL:-http://localhost:3000}"
DAGSTER_JOB="${DAGSTER_JOB:-tpcds_etl_job}"
DAGSTER_REPO="${DAGSTER_REPO:-tpcds_etl}"

# Snowflake defaults (Airflow-based orchestration)
SNOWFLAKE_DAG_ID="${SNOWFLAKE_DAG_ID:-tpcds_etl_snowflake}"

# dbt defaults
DBT_PROJECT_DIR="${DBT_PROJECT_DIR:-$ROOT_DIR/dbt_duckdb}"

mkdir -p "$RESULTS_DIR"

log() { echo "=== [$(date -u +%H:%M:%S)] $*"; }

# ---------------------------------------------------------------------------
# Flush caches (best-effort, requires root)
# ---------------------------------------------------------------------------
flush_caches() {
    if [[ "$(id -u)" -eq 0 ]]; then
        log "Flushing page cache"
        echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
        sync
    else
        log "Not root -- skipping cache flush"
    fi
}

# ---------------------------------------------------------------------------
# Start services
# ---------------------------------------------------------------------------
start_services() {
    local compose_dir="$ROOT_DIR/$COMPETITOR"

    if [[ "$MODE" == "k8s" ]]; then
        log "Deploying $COMPETITOR via Helm/k8s"
        if [[ -d "$ROOT_DIR/$COMPETITOR/k8s" ]]; then
            kubectl apply -f "$ROOT_DIR/$COMPETITOR/k8s/" --namespace bench
        elif [[ -d "$ROOT_DIR/$COMPETITOR/helm" ]]; then
            helm upgrade --install "$COMPETITOR" "$ROOT_DIR/$COMPETITOR/helm/" \
                --namespace bench --create-namespace \
                --wait --timeout 300s
        fi
    else
        if [[ -f "$compose_dir/docker-compose.yml" ]]; then
            log "Starting $COMPETITOR via docker compose"
            docker compose -f "$compose_dir/docker-compose.yml" up -d --wait
        else
            log "No docker-compose.yml found for $COMPETITOR at $compose_dir"
        fi
    fi
}

# ---------------------------------------------------------------------------
# Wait for service readiness
# ---------------------------------------------------------------------------
wait_ready() {
    local url="$1"
    local max_wait="${2:-120}"
    local elapsed=0

    log "Waiting for $url to be ready (max ${max_wait}s)"
    while ! curl -sf "$url" > /dev/null 2>&1; do
        sleep 2
        elapsed=$((elapsed + 2))
        if [[ $elapsed -ge $max_wait ]]; then
            log "ERROR: $url not ready after ${max_wait}s"
            return 1
        fi
    done
    log "$url is ready (${elapsed}s)"
}

# ---------------------------------------------------------------------------
# Trigger pipeline and collect results
# ---------------------------------------------------------------------------
run_windmill() {
    wait_ready "$WM_BASE_URL/api/version"

    log "Triggering Windmill flow: $WM_FLOW_PATH"
    local response
    response=$(curl -sf -X POST \
        "$WM_BASE_URL/api/w/$WM_WORKSPACE/jobs/run/f/$WM_FLOW_PATH" \
        -H "Authorization: Bearer $WM_TOKEN" \
        -H "Content-Type: application/json" \
        -d '{}')
    local job_id
    job_id=$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin))" 2>/dev/null || echo "$response" | tr -d '"')

    log "Flow job started: $job_id"
    log "Waiting for completion..."

    while true; do
        local status
        status=$(curl -sf "$WM_BASE_URL/api/w/$WM_WORKSPACE/jobs_u/completed/get/$job_id" 2>/dev/null && echo "done" || echo "running")
        if [[ "$status" == *"done"* ]]; then
            break
        fi
        sleep 5
    done

    log "Flow completed. Collecting timings."
    python3 "$SCRIPT_DIR/collect_windmill.py" \
        --job-id "$job_id" \
        --output "$RESULTS_DIR/windmill_${TIMESTAMP}.json"
}

run_airflow() {
    wait_ready "$AIRFLOW_BASE_URL/health"

    log "Triggering Airflow DAG: $AIRFLOW_DAG_ID"
    local response
    response=$(curl -sf -X POST \
        "$AIRFLOW_BASE_URL/api/v1/dags/$AIRFLOW_DAG_ID/dagRuns" \
        -u "$AIRFLOW_USER:$AIRFLOW_PASSWORD" \
        -H "Content-Type: application/json" \
        -d "{\"conf\": {}}")
    local run_id
    run_id=$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin)['dag_run_id'])")

    log "DAG run started: $run_id"
    log "Waiting for completion..."

    while true; do
        local state
        state=$(curl -sf \
            "$AIRFLOW_BASE_URL/api/v1/dags/$AIRFLOW_DAG_ID/dagRuns/$run_id" \
            -u "$AIRFLOW_USER:$AIRFLOW_PASSWORD" \
            | python3 -c "import sys,json; print(json.load(sys.stdin)['state'])")
        if [[ "$state" == "success" || "$state" == "failed" ]]; then
            log "DAG run finished with state: $state"
            break
        fi
        sleep 5
    done

    python3 "$SCRIPT_DIR/collect_airflow.py" \
        --run-id "$run_id" \
        --output "$RESULTS_DIR/airflow_${TIMESTAMP}.json"
}

run_dagster() {
    wait_ready "$DAGSTER_BASE_URL"

    log "Triggering Dagster job: $DAGSTER_JOB"
    local mutation
    mutation='mutation { launchRun(executionParams: { selector: { repositoryName: "'"$DAGSTER_REPO"'", repositoryLocationName: "'"$DAGSTER_REPO"'", jobName: "'"$DAGSTER_JOB"'" }, runConfigData: {} }) { ... on LaunchRunSuccess { run { runId } } ... on PythonError { message } } }'

    local response
    response=$(curl -sf -X POST "$DAGSTER_BASE_URL/graphql" \
        -H "Content-Type: application/json" \
        -d "{\"query\": \"$mutation\"}")
    local run_id
    run_id=$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['launchRun']['run']['runId'])")

    log "Dagster run started: $run_id"
    log "Waiting for completion..."

    while true; do
        local status_query='query { runOrError(runId: "'"$run_id"'") { ... on Run { status } } }'
        local state
        state=$(curl -sf -X POST "$DAGSTER_BASE_URL/graphql" \
            -H "Content-Type: application/json" \
            -d "{\"query\": \"$status_query\"}" \
            | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['runOrError']['status'])")
        if [[ "$state" == "SUCCESS" || "$state" == "FAILURE" ]]; then
            log "Dagster run finished with status: $state"
            break
        fi
        sleep 5
    done

    python3 "$SCRIPT_DIR/collect_dagster.py" \
        --run-id "$run_id" \
        --output "$RESULTS_DIR/dagster_${TIMESTAMP}.json"
}

run_dbt() {
    log "Running dbt in $DBT_PROJECT_DIR"
    pushd "$DBT_PROJECT_DIR" > /dev/null

    dbt run --profiles-dir . --project-dir .

    local results_file="$DBT_PROJECT_DIR/target/run_results.json"
    if [[ ! -f "$results_file" ]]; then
        log "ERROR: run_results.json not found at $results_file"
        popd > /dev/null
        return 1
    fi

    python3 "$SCRIPT_DIR/collect_dbt.py" \
        --results-file "$results_file" \
        --output "$RESULTS_DIR/dbt_${TIMESTAMP}.json"

    popd > /dev/null
}

run_snowflake() {
    wait_ready "$AIRFLOW_BASE_URL/health"

    log "Triggering Snowflake DAG: $SNOWFLAKE_DAG_ID"
    local response
    response=$(curl -sf -X POST \
        "$AIRFLOW_BASE_URL/api/v1/dags/$SNOWFLAKE_DAG_ID/dagRuns" \
        -u "$AIRFLOW_USER:$AIRFLOW_PASSWORD" \
        -H "Content-Type: application/json" \
        -d "{\"conf\": {}}")
    local run_id
    run_id=$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin)['dag_run_id'])")

    log "DAG run started: $run_id"
    log "Waiting for completion..."

    while true; do
        local state
        state=$(curl -sf \
            "$AIRFLOW_BASE_URL/api/v1/dags/$SNOWFLAKE_DAG_ID/dagRuns/$run_id" \
            -u "$AIRFLOW_USER:$AIRFLOW_PASSWORD" \
            | python3 -c "import sys,json; print(json.load(sys.stdin)['state'])")
        if [[ "$state" == "success" || "$state" == "failed" ]]; then
            log "DAG run finished with state: $state"
            break
        fi
        sleep 5
    done

    python3 "$SCRIPT_DIR/collect_snowflake.py" \
        --dag-id "$SNOWFLAKE_DAG_ID" \
        --run-id "$run_id" \
        --output "$RESULTS_DIR/snowflake_${TIMESTAMP}.json"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
log "Benchmark: $COMPETITOR (mode: $MODE)"
log "Results dir: $RESULTS_DIR"

start_services
flush_caches

case "$COMPETITOR" in
    windmill)  run_windmill ;;
    airflow)   run_airflow ;;
    dagster)   run_dagster ;;
    dbt)       run_dbt ;;
    snowflake) run_snowflake ;;
    *)
        echo "ERROR: Unknown competitor '$COMPETITOR'"
        echo "Usage: $0 <windmill|airflow|dagster|dbt|snowflake> [docker|k8s]"
        exit 1
        ;;
esac

log "Done. Results saved to $RESULTS_DIR/"
ls -la "$RESULTS_DIR/"*.json 2>/dev/null || true
