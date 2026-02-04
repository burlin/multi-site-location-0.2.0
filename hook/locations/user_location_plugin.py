"""Module for configuring user locations based on config yaml file"""
import logging
import os
import sys
import getpass
import socket
import platform
import itertools
import functools
from pathlib import Path
from typing import Optional, Dict, Any

import ftrack_api # type: ignore

logger = logging.getLogger(
    'ftrack_connect.multi_site_location.user_location_plugin'
)

dependencies_path = Path(__file__).parent.parent.parent / 'dependencies'
# logger.debug(f"Dependencies path: {dependencies_path}")
if dependencies_path.exists() and str(dependencies_path) not in sys.path:
    sys.path.insert(0, str(dependencies_path))

import yaml # type: ignore  # noqa: E402
from jinja2 import Template  # type: ignore # noqa: E402


LOCATIONS_CONFIG_PATH = Path(__file__).parent / 'disk_locations.yaml'

def get_hostname() -> str:
    try:
        return socket.gethostname()
    except Exception:
        try:
            return platform.node()
        except Exception:
            return os.environ.get('COMPUTERNAME', 
                os.environ.get('HOSTNAME', 'unknown'))
            
def load_location_config(config_path: Path, user_name: Optional[str]) -> dict[str, Any]:
    """Load and parse location configuration from YAML file with Jinja template support.
    
    Important:
        - We treat location names as *keys* in the ``locations`` mapping.
        - After Jinja+YAML, some keys may end up as ``''``/whitespace or even ``None``
          (например, если env‑переменная в шаблоне пустая).
        - Такие записи мы отбрасываем ЗДЕСЬ, чтобы далее в код не просочился
          пустой Location.name, который приводит к IntegrityError
          ``Duplicate entry '' for key 'name'`` на сервере.
    """
    # Read the YAML file
    with open(config_path, 'r') as f:
        yaml_content = f.read()
    
    # Create Jinja template
    template = Template(yaml_content)
    
    # Define template variables
    template_vars = {
        'USER_NAME': user_name if user_name else getpass.getuser(),
        'HOST_NAME': get_hostname(),
        'HOME': str(Path.home()),
        'FTRACK_LOCAL_STORAGE': os.environ.get(
            'FTRACK_LOCAL_STORAGE', 
            str(Path.home() / 'ftrack_local_storage')
        ),
    }
    
    rendered_yaml = template.render(**template_vars)
    config = yaml.safe_load(rendered_yaml) or {}
    raw_locations = config.get('locations', {}) or {}
    
    cleaned_locations: dict[str, Any] = {}
    for raw_name, location_data in raw_locations.items():
        # Normalize key to a clean string
        name = (str(raw_name) if raw_name is not None else '').strip()
        if not name:
            logger.warning(
                "Skipping location with empty/invalid name in %s "
                "(key=%r, data=%r) to avoid IntegrityError for Location.name.",
                config_path,
                raw_name,
                location_data,
            )
            continue
        cleaned_locations[name] = location_data or {}
    
    logger.debug('Loaded configuration with resolved templates. Locations: %s', cleaned_locations)
    return cleaned_locations

def session_add_user_location(
    session: ftrack_api.Session, 
    location_setup: Dict[str, Any]
) -> None:
    """Add user-specific location to the session based on disk_locations.yaml.
    
    Args:
        session: ftrack_api.Session object
        location_setup: dict of locations to configure, loaded from disk_locations.yaml
    """
    index = itertools.count()
    for location_name, location_data in location_setup.items():
        location_name = (location_name or '').strip()
        if not location_name:
            logger.warning(
                "Skipping location with empty name in config "
                "(avoids IntegrityError 'Duplicate entry \"\" for key \"name\"')."
            )
            continue
        location = session.ensure('Location', {'name': location_name})
        prefix_data = location_data.get('prefix', {})
        location_path = prefix_data.get(sys.platform, '')
        if not location_path:
            logger.warning(f'No path configured for location {location_name} on platform {sys.platform}')
            continue
            
        location.accessor = ftrack_api.accessor.disk.DiskAccessor(prefix=location_path)
        location.structure = ftrack_api.structure.standard.StandardStructure()
        location.priority = location_data.get('priority', next(index))
        logger.info(f'Registered location {location_name} with path {location.accessor.prefix} (priority: {location.priority})')        

def configure_locations(
    event: ftrack_api.event.base.Event, 
    location_setup: Optional[Dict[str, Any]] = None
):
    """Configure locations based on disk_locations.yaml
    
    Args:
        event: ftrack_api.Event
        location_setup: dict of locations to configure (optional, will load from YAML if not provided)

    """
    # Filter by hostname to prevent registering locations from other workstations
    # where the same user is logged in
    current_hostname = get_hostname().lower()
    event_source = event.get('source', {})
    event_hostname = event_source.get('hostname', '').lower()
    
    # If hostname is set in event source, only process if it matches current hostname
    if event_hostname and event_hostname != current_hostname:
        logger.debug(
            "Skipping location configuration: event from hostname '%s', "
            "current hostname: '%s'",
            event_hostname,
            current_hostname
        )
        return
    
    session = event['data']['session']
    user_name = session.api_user
    # If no location_setup provided, load from YAML
    if location_setup is None:
        if not LOCATIONS_CONFIG_PATH.exists():
            logger.error(f'Configuration file not found: {LOCATIONS_CONFIG_PATH}')
            return
        location_setup = load_location_config(
            config_path=LOCATIONS_CONFIG_PATH, 
            user_name=user_name
        )
        
    logger.info(
        "Configuring user locations for hostname '%s' (event hostname: '%s')",
        current_hostname,
        event_hostname if event_hostname else 'not set'
    )

    session_add_user_location(
        session, 
        location_setup=location_setup
    )
    logger.info("User locations configured successfully.")
    
def register(api_object, **kw):
    """Register the user location plugin.
    
    Every time a new session is created, this plugin will be called.
    All the files like hook/**/*.py which has `register` function will be registered.
    
    Args:
        session: ftrack session
        kw: keyword arguments
    Returns:
        None
    Notes:
        In next Ftrack connect version there will be released source.host parameter
        which will be used to filter out events from other hosts.
        https://github.com/ftrackhq/ftrack-python/pull/40/files#diff-ccff1a731bb5b3fd2f6e195d150961d043b65ce565980e85006cf065d4264721R594
        https://github.com/ftrackhq/ftrack-python/releases/tag/v3.0.4rc1
        If you want to start playing and using the new features you can install the latest rc api in a virtual environment and make sure to run your envents from there.
        `pip install --pre --upgrade ftrack-python-api`
        For using from Connect, it need to be recompiled with the latest api. 
    """
    logger.info("========= Registering USER location plugin ==========")
    if not isinstance(api_object, ftrack_api.Session):
        return
    # Subscribe to the event hub
    locations_dct = load_location_config(config_path=LOCATIONS_CONFIG_PATH, user_name=api_object.api_user)
    
    api_object.event_hub.subscribe(
        'topic=ftrack.api.session.configure-location',
        # 'topic=ftrack.api.session.configure-location and source.user={0}'.format(api_object.api_user),
        functools.partial(
            configure_locations, 
            location_setup=locations_dct
        ),
        priority=10
    )