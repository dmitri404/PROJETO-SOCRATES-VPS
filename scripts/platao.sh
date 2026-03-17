#!/bin/bash

LOG="/opt/portal/logs/platao.log"
LOCKFILE=/tmp/socrates.lock

# Garante execucao unica
exec 200>"$LOCKFILE"
flock -n 200 || {
    echo "=== $(date) === socrates ja esta rodando, ignorando." >> "$LOG"
    exit 0
}

echo "=== $(date) === Iniciando socrates via platao.sh" >> "$LOG"
cd /opt/portal
docker compose run --rm socrates >> "$LOG" 2>&1
echo "=== $(date) === socrates finalizado" >> "$LOG"
