import os
import yaml
import logging

# Default configuration settings
HARDCODED_EXCLUSIONS = [
    'snapraid',
    '.snapraid',
    '.content'
]

DEFAULT_CONFIG = {
    'Paths': {
        'LOG_PATH': '/var/log/cache-mover.log'
    },
    'Settings': {
        'AUTO_UPDATE': False,
        'THRESHOLD_PERCENTAGE': 70,
        'TARGET_PERCENTAGE': 25,
        'MAX_WORKERS': 8,
        'MAX_LOG_SIZE_MB': 100,
        'BACKUP_COUNT': 1,
        'UPDATE_BRANCH': 'main',
        'EXCLUDED_DIRS': HARDCODED_EXCLUSIONS,
        'SCHEDULE': '0 3 * * *',
        'NOTIFICATIONS_ENABLED': False,
        'NOTIFICATION_URLS': [],
        'NOTIFY_THRESHOLD': False
    }
}

def get_script_dir():
    """Get the directory where the script is located."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def load_config():
    """
    Load and validate configuration from config.yml and environment variables.
    
    Returns:
        dict: The merged and validated configuration
    
    Raises:
        ValueError: If required paths are missing or threshold/target values are invalid
    """
    config = DEFAULT_CONFIG.copy()
    
    # Load from config file
    script_dir = get_script_dir()
    config_path = os.path.join(script_dir, 'config.yml')
    if os.path.exists(config_path):
        with open(config_path, 'r') as config_file:
            file_config = yaml.safe_load(config_file)
            config['Paths'].update(file_config.get('Paths', {}))
            
            user_exclusions = file_config.get('Settings', {}).get('EXCLUDED_DIRS', [])
            if isinstance(user_exclusions, str):
                user_exclusions = [x.strip() for x in user_exclusions.split(',')]
            elif not isinstance(user_exclusions, list):
                user_exclusions = [str(user_exclusions)] if user_exclusions else []
            
            combined_exclusions = list(set(HARDCODED_EXCLUSIONS + user_exclusions))
            
            settings_update = file_config.get('Settings', {})
            settings_update['EXCLUDED_DIRS'] = combined_exclusions
            config['Settings'].update(settings_update)

    # Environment variable mappings
    env_mappings = {
        'CACHE_PATH': ('Paths', 'CACHE_PATH'),
        'BACKING_PATH': ('Paths', 'BACKING_PATH'),
        'LOG_PATH': ('Paths', 'LOG_PATH'),
        'THRESHOLD_PERCENTAGE': ('Settings', 'THRESHOLD_PERCENTAGE', float),
        'TARGET_PERCENTAGE': ('Settings', 'TARGET_PERCENTAGE', float),
        'MAX_WORKERS': ('Settings', 'MAX_WORKERS', int),
        'MAX_LOG_SIZE_MB': ('Settings', 'MAX_LOG_SIZE_MB', int),
        'BACKUP_COUNT': ('Settings', 'BACKUP_COUNT', int),
        'UPDATE_BRANCH': ('Settings', 'UPDATE_BRANCH', str),
        'EXCLUDED_DIRS': ('Settings', 'EXCLUDED_DIRS', lambda x: list(set(HARDCODED_EXCLUSIONS + [y.strip() for y in x.split(',')]) if x else HARDCODED_EXCLUSIONS)),
        'SCHEDULE': ('Settings', 'SCHEDULE', str),
        'NOTIFICATIONS_ENABLED': ('Settings', 'NOTIFICATIONS_ENABLED', lambda x: x.lower() == 'true'),
        'NOTIFICATION_URLS': ('Settings', 'NOTIFICATION_URLS', lambda x: x.split(',')),
        'NOTIFY_THRESHOLD': ('Settings', 'NOTIFY_THRESHOLD', lambda x: str(x).lower() == 'true' if x is not None else False),
    }

    # Apply environment variables
    for env_var, (section, key, *convert) in env_mappings.items():
        env_value = os.environ.get(env_var)
        if env_value is not None:
            if convert:
                env_value = convert[0](env_value)
            config[section][key] = env_value

    # Docker-specific settings
    if os.environ.get('DOCKER_CONTAINER'):
        config['Settings']['AUTO_UPDATE'] = False
        config['Settings']['MAX_LOG_SIZE_MB'] = 100
        config['Settings']['BACKUP_COUNT'] = 1

    # Validate required paths
    required_paths = ['CACHE_PATH', 'BACKING_PATH']
    missing_paths = [path for path in required_paths 
                    if not config['Paths'].get(path)]
    
    if missing_paths:
        raise ValueError(f"Required paths not configured: {', '.join(missing_paths)}. "
                        f"Please set via config.yml or environment variables.")

    # Validate threshold and target percentages
    threshold = config['Settings']['THRESHOLD_PERCENTAGE']
    target = config['Settings']['TARGET_PERCENTAGE']
    
    if threshold == 0 and target == 0:
        logging.info("Both THRESHOLD_PERCENTAGE and TARGET_PERCENTAGE are 0. Cache will be emptied completely.")
    elif threshold <= target:
        raise ValueError("THRESHOLD_PERCENTAGE must be greater than TARGET_PERCENTAGE (or both must be 0 to empty cache completely)")

    return config 