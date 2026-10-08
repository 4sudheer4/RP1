#!/usr/bin/env bash
# Runs every Stage 5 experiment in sequence (~75 min on a laptop). Each writes docs/results/stage5/<name>/<ts>/.
# Prereqs: compose stack up (docker compose -f infra/docker-compose.yml up -d --wait) and jars built
# (mvn -q install -DskipTests). Keep the machine awake and on power.
if [[ $(uname) == Darwin && -z ${RP1_CAFFEINATED:-} ]]; then RP1_CAFFEINATED=1 exec caffeinate -i "$0" "$@"; fi
set -uo pipefail
cd "$(dirname "$0")"
step() { echo "##### $* $(date +%T)"; "$@" || echo "##### FAILED $*"; }
step ./exp-a-cliff.sh
step ./exp-a-cliff.sh --ens-suspension --rates "40 70 100"
step ./exp-b-broker-kill.sh
step ./exp-c-toxiproxy.sh
step ./exp-d-poison.sh
step ./exp-e-ordering-rebalance.sh
step ./exp-f-lag.sh
echo "##### ALL DONE $(date +%T)"
