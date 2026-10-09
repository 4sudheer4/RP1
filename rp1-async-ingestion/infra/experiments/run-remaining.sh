#!/usr/bin/env bash
# Re-runs what was invalidated by laptop sleep on 2026-10-08 and the experiments not yet run, with the fixed
# breaker/bulkhead config. ~60 min. Plug in, keep the lid open.
if [[ $(uname) == Darwin && -z ${RP1_CAFFEINATED:-} ]]; then RP1_CAFFEINATED=1 exec caffeinate -dims "$0" "$@"; fi
set -uo pipefail
cd "$(dirname "$0")"
step() { echo "##### $* $(date +%T)"; "$@" || echo "##### FAILED $*"; }
step ./exp-a-cliff.sh --modes async --rates "80 100 120"
step ./exp-a-cliff.sh --ens-suspension --rates "40 70 100"
step ./exp-b-broker-kill.sh
step ./exp-c-toxiproxy.sh
step ./exp-d-poison.sh
step ./exp-e-ordering-rebalance.sh
step ./exp-f-lag.sh
echo "##### ALL DONE $(date +%T)"
