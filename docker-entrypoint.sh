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

printenv | grep -v "no_proxy" > /etc/environment

echo "$SCHEDULE BASH_ENV=/etc/environment /usr/local/bin/python /app/cache-mover.py --console-log >> /proc/1/fd/1 2>&1" > /etc/cron.d/cache-mover
echo "" >> /etc/cron.d/cache-mover
chmod 0644 /etc/cron.d/cache-mover

crontab /etc/cron.d/cache-mover

run_cache_mover
cron -f &
tail -f /var/log/cache-mover.log