#!/bin/bash
run_cache_mover() {
    python /app/cache-mover.py --console-log
}

SCHEDULE_FROM_CONFIG=$(python3 -c "
import yaml
try:
    with open('/app/config.yml', 'r') as f:
        config = yaml.safe_load(f)
        print(config.get('Settings', {}).get('SCHEDULE', '0 3 * * *'))
except:
    print('0 3 * * *')
")

SCHEDULE=${SCHEDULE:-$SCHEDULE_FROM_CONFIG}

echo "$SCHEDULE /usr/local/bin/python /app/cache-mover.py --console-log" > /etc/cron.d/cache-mover
chmod 0644 /etc/cron.d/cache-mover

apt-get update && apt-get install -y cron

cron

run_cache_mover

tail -f /var/log/cache-mover.log