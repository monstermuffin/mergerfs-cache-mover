### v1.1 - 2025-01-20
  - Apprise support added.

### v1.0 - 2025-01-17
  - Added Docker support with scheduling and process management.
  - Added empty cache mode by setting both THRESHOLD_PERCENTAGE and TARGET_PERCENTAGE to 0.
  - Added hardcoded snapraid file/directory exclusions.
  - Added graceful shutdown handling for Docker containers.
  - Updated `load_config()` to handle Docker environment variable.
  - Added `docker-entrypoint.sh` for container scheduling.
  - Added Docker-specific process detection.
  - Added Docker environment variable configuration support.
  - Added Docker volume mount support for logs and configuration.
  - Added SCHEDULE option in config.yml (Docker-only).
  - Updated process detection for containerized environment.
  - Disabled auto-updates in Docker containers.
  - Enhanced logging for Docker operations.
  - Added Docker documentation.

### v0.98.7
 - Added a custom `HybridFormatter` class for more detailed logging of file move operations.
 - Implemented a new `set_git_dir()` function to set the Git directory environment variable.
 - Added support for specifying the update branch in the configuration (`UPDATE_BRANCH`).
 - Introduced excluded directories feature (`EXCLUDED_DIRS`in config). By default the `Snapraid` folder is excluded as there were instances of this being moved.
 - Added graceful shutdown handling using signal handlers (`SIGINT` and `SIGTERM`). This could possibly use more work to be honest.
 - Implemented free space checking before file moves.
 - Enhanced `auto_update()` function with better error handling and logging.
 - Refactored `move_file()` function for better error handling and logging.
 - Addressed potential race condition in checking filesystem usage with a lock mechanism. This should have cleared log spamming by the threads too. 
 - Added validation to ensure `THRESHOLD_PERCENTAGE` is greater than `TARGET_PERCENTAGE`.

 #### TODO:
 - Replace `os.walk()` with `os.scandir()`.

### v0.97
 - Re-added excluded dirs option. This is required as my Snapraid dir was moved with the content file. By default, the `snapraid` dir is now excluded. 
 - Fixed autoupdates not working when run automatically. 
 - Remove logging whilst gathering files as this was spamming the log and had no real use. 
 - Added check to ensure `THRESHOLD_PERCENTAGE` is greater than `TARGET_PERCENTAGE`.

### v0.96.5
 - Fixed child process detection

 ### v0.96
 - Fixed existing process detection


### v0.95
  - Fixed accidental directory collapse in backend pool upon directory manipulation
  - Replaced rsync with Python's built-in file operations for better control and compatibility
  - Added explicit permission and ownership preservation
  - Added --dry-run option for testing without file movement
  - "Improved" empty directory removal process
  - Enhanced logging


### v0.92
- Enhanced rsync command in move_file() function:
  - Added --preallocate option to improve performance and reduce fragmentation
  - Added --hard-links option to preserve hard links during file transfers
- Updated README to reflect new rsync options


### v0.91
- Simplified permission handling in the move_file() function
- Updated rsync command to use --perms option for explicit permission preservation
  - Now using --mkpath to resolve issues with base path not existing on destination
- Deprecated USER, GROUP, FILE_CHMOD, and DIR_CHMOD settings from config.yml
- Updated README

### v0.88
- Fixed auto-update functionality
  - Resolved issues when run via cron/systemd or outside script directory
  - Added AUTO_UPDATE configuration option to enable/disable auto-updates
- Improved script reliability
  - Added get_script_dir() function for consistent script directory detection
  - Modified get_current_commit_hash() to use the script's directory
  - Updated auto_update() function to use the script's directory for Git operations

### v0.83
- Added auto-update feature
  - The script now checks for updates from the GitHub repository
  - Automatically updates itself if a new version is available
- Improved logging
  - Added more detailed logging for the update process
- Code refactoring and optimization
  - Much tidier now my Python is slightly less shit, hopefully didn't break anything

### v0.7
- Initial release of the mergerfs-cache-mover script
- Basic functionality for moving files from cache to backing storage
- Configurable settings via config.yml
- Logging with rotation
- Support for both Systemd timer and Crontab scheduling