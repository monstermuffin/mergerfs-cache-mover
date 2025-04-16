import os
import sys
import logging
import subprocess
import requests

def get_script_dir():
    """Get the directory where the script is located."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def set_git_dir():
    """Set the GIT_DIR environment variable."""
    script_dir = get_script_dir()
    os.environ['GIT_DIR'] = os.path.join(script_dir, '.git')

def get_current_commit_hash():
    """
    Get the current git commit hash.
    Returns None in Docker containers or if unable to get hash.
    """
    # Skip git operations in Docker containers
    if os.environ.get('DOCKER_CONTAINER'):
        logging.debug("Running in Docker container, skipping git commit hash check")
        return None

    set_git_dir()
    try:
        result = subprocess.run(['git', 'rev-parse', 'HEAD'],
                              capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logging.error(f"Error getting current commit hash: {e}")
        return None

def run_git_command(command, error_message):
    """Run a git command and handle errors."""
    # Skip git operations in Docker containers
    if os.environ.get('DOCKER_CONTAINER'):
        logging.debug("Running in Docker container, skipping git operations")
        return None

    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return result
    except subprocess.CalledProcessError as e:
        logging.error(f"{error_message} Command: {e.cmd}")
        logging.error(f"Error output: {e.stderr}")
        raise

def auto_update(config):
    """
    Check for updates and automatically update the script if available.
    Skips update check in Docker containers.
    
    Args:
        config (dict): Configuration dictionary
    
    Returns:
        bool: True if update was successful or not needed, False if update failed
    """
    # Skip auto-update in Docker containers
    if os.environ.get('DOCKER_CONTAINER'):
        logging.debug("Running in Docker container, auto-update disabled")
        return True

    set_git_dir()
    current_commit = get_current_commit_hash()
    if not current_commit:
        logging.warning("Unable to get current commit hash. Skipping auto-update.")
        return False

    update_branch = config['Settings'].get('UPDATE_BRANCH', 'main')
    
    try:
        api_url = f"https://api.github.com/repos/MonsterMuffin/mergerfs-cache-mover/commits/{update_branch}"
        response = requests.get(api_url)
        response.raise_for_status()
        latest_commit = response.json()['sha']

        if latest_commit != current_commit:
            logging.info(f"A new version is available on branch '{update_branch}'. Current: {current_commit[:7]}, Latest: {latest_commit[:7]}")
            logging.info("Attempting to auto-update...")

            try:
                run_git_command(['git', 'fetch', 'origin', update_branch],
                              f"Failed to fetch updates from {update_branch}.")
                run_git_command(['git', 'reset', '--hard', f'origin/{update_branch}'],
                              f"Failed to reset to latest commit on {update_branch}.")

                logging.info("Update successful. Restarting script...")
                os.execv(sys.executable, [sys.executable] + sys.argv)
            except subprocess.CalledProcessError:
                return False
        else:
            logging.info(f"Already running the latest version on branch '{update_branch}' (commit: {current_commit[:7]}).")

        return True
    except requests.RequestException as e:
        logging.error(f"Failed to check for updates: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error during update process: {e}")
        return False 