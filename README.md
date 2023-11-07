# mergerfs-cache-mover
Python script for moving files on a cached disk to a backing mergerFS disk pool.

More information in this blog post:

https://blog.muffn.io/posts/part-4-100tb-mini-nas/ (if that link doesn't work it's not released yet.)

## How It Works
The script operates by checking the disk usage of the cache directory. If the usage is above the threshold percentage defined in the configuration file (`config.yml`), it will move the oldest files out to the backing storage location until the usage is below a defined target percentage. Empty directories are also cleaned up after files are moved.

The script uses a configuration file to manage settings such as paths, thresholds, and system parameters. It also checks for other instances of itself to prevent multiple concurrent operations, in the event a move process is still occurring from a previous run because you are using prehistoric storage or something.

## Logging
The script logs its operations, which includes information on moved files, errors, and other warnings. The logs are rotated based on the file size and backup count defined in config.yml.

## Requirements
- Python 3.6 or higher
- PyYAML (to be installed from `requirements.txt`)

## Setup
1. To get started, clone the repository to your local machine using the following command:
```shell
git clone https://github.com/MonsterMuffin/mergerfs-cache-mover.git
```

2. Install the required Python package using pip:
```shell
pip install -r requirements.txt
```

## Configuration Setup
Copy `config.example.yml` to `config.yml` and set up your `config.yml` with the appropriate values:

- `CACHE_PATH`: The path to your cache directory. !!THIS IS YOUR CACHE DISK ROOT, NOT MERGERFS CACHE MOUNT!!
- `BACKING_PATH`: The path to the backing storage where files will be moved.
- `LOG_PATH`: The path for the log file generated by the script.
- `THRESHOLD_PERCENTAGE`: The usage percentage of the cache directory that triggers the file-moving process.
- `TARGET_PERCENTAGE`: The target usage percentage to achieve after moving files.
- `MAX_WORKERS`: The maximum number of parallel file-moving operations.
- `MAX_LOG_SIZE_MB`: The maximum size for the log file before it's rotated.
- `BACKUP_COUNT`: The number of backup log files to maintain.
- `USER`: The username that should have ownership of the files.
- `GROUP`: The group that should have ownership of the files.
- `CHMOD`: The permissions to set for the above user/group on all files moved.

## Usage
To run the script, use the following command from your terminal:

```shell
python3 cache-mover.py
```

Of course, this is meant to be run automatically....

## Automated Execution

Use either a Systemd timer or Crontab entry. I have been moving from crontab to systemd timers myself, but you live your life how you see fit.

### Option 1: Systemd Timer
1. Create a systemd service file `/etc/systemd/system/cache_mover.service`. Change `/path/to/cache-mover.py` to where you downloaded the script, obviously.

```ini
[Unit]
Description="Muffin's Cache Mover Script."

[Service]
Type=oneshot
ExecStart=/usr/bin/python3 /path/to/cache-mover.py
```

2. Create a systemd timer file /etc/systemd/system/cache_mover.timer. The timer format is not the usual crontab format, [find out more](https://silentlad.com/systemd-timers-oncalendar-(cron)-format-explained) if you need help.

```ini
[Unit]
Description="Runs Muffin's Cache Mover Script Daily at 3AM."

[Timer]
OnCalendar=*-*-* 03:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

3. Enable and start the timer:

```shell
systemctl enable cache_mover.timer
systemctl start cache_mover.timer
```

4. Check timer status:

```shell
systemctl list-timers
```

### Option 2: Crontab

1. Open crontab file for editing:

```shell
sudo crontab -e
```

2. Add line to run script. The following example will run the script daily, at 3AM. You can adjust this by using a site such as [crontab.guru.](https://crontab.guru/)

Change `/path/to/cache-mover.py` to where you downloaded the script, obviously.

```cron
0 3 * * * /usr/bin/python3 /path/to/cache-mover.py
```

## Fin.

I take no responsibility for what my shitty coding does to your data. This has been working well for me, but always take care.