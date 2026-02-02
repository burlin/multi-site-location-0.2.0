import logging
import os
import socket
from functools import partial
from pathlib import Path
from typing import Any, Dict, Union

import ftrack_api # type: ignore

logger = logging.getLogger("ftrack.plugins.multi-site-location")

FTRACK_EVENT_PLUGIN_PATH = 'FTRACK_EVENT_PLUGIN_PATH'
FTRACK_EVENT_PLUGIN_TOPICS = [
    'topic=ftrack.connect.application.launch',  # DCC applications
    'topic=ftrack.action.launch',                # Actions
]

LOCATION_PLUGIN_PATH = Path(__file__).parent / 'locations'

def append_path_to_environment(
    environment: Dict[str, str],
    key: str, 
    path: Union[str, Path] 
) -> Dict[str, str]:
    """Append path to environment variable, creating it if it doesn't exist."""
    current_value = environment.get(key, "")
    environment[key] = os.pathsep.join([current_value, str(path)]) if current_value else str(path)
    return environment


def add_path_to_event_plugins(event: ftrack_api.event.base.Event, path: Path) -> None:
    """Add location plugin to event plugins.
    
    Args:
        event: The ftrack event to modify
    """
    # Ensure the event data structure exists
    event_data = event.setdefault('data', {})
    options = event_data.setdefault('options', {})
    env = options.setdefault('env', {})
    
    
    append_path_to_environment(env, 
        'FTRACK_EVENT_PLUGIN_PATH',
        path
    )
    logger.debug(f'Added {path} to FTRACK_EVENT_PLUGIN_PATH')


def _patch_event_hub_publish(session: ftrack_api.Session) -> None:
    """Patch event_hub.publish to add hostname to ftrack.api.session.configure-location events.
    
    This ensures that location plugins can filter events by hostname to prevent
    registering locations from other workstations where the same user is logged in.
    
    Args:
        session: The ftrack API session object
    """
    original_publish = session.event_hub.publish
    
    def publish_with_hostname(event, *args, **kwargs):
        """Wrapper around event_hub.publish that adds hostname to configure-location events."""
        # Add hostname to source for ftrack.api.session.configure-location events
        if event.get('topic') == 'ftrack.api.session.configure-location':
            try:
                current_hostname = socket.gethostname().lower()
                event.setdefault('source', {})['hostname'] = current_hostname
                logger.debug(
                    "Added hostname '%s' to ftrack.api.session.configure-location event source",
                    current_hostname
                )
            except Exception as e:
                logger.warning("Failed to add hostname to event source: %s", e)
        
        return original_publish(event, *args, **kwargs)
    
    # Replace the publish method
    session.event_hub.publish = publish_with_hostname
    logger.info("Patched event_hub.publish to add hostname to configure-location events")


def register(api_object: ftrack_api.Session, **kw: Any) -> None:
    """Register the multi-site location plugin.
    
    This method ensures that the plugin is loaded and all modules in 
    subfolders `hook/**/*.py` with a `register` method are registered.
    
    Args:
        api_object: The ftrack API session object
        **kw: Additional keyword arguments
    """
    logger.info("~~~ Multi-Site Location Plugin successfully registered ~~~")
    logger.info("This plugin enables synchronization of assets across multiple storage locations")
    
    if not isinstance(api_object, ftrack_api.Session):
        logger.warning("Expected ftrack_api.Session instance, got %s", type(api_object).__name__)
        return
    
    # Patch event_hub.publish to add hostname to configure-location events
    _patch_event_hub_publish(api_object)
    
    # Subscribe to events for both DCC applications and actions
    for topic in FTRACK_EVENT_PLUGIN_TOPICS:
        api_object.event_hub.subscribe(
            topic, 
            partial(
                add_path_to_event_plugins, 
                path=LOCATION_PLUGIN_PATH
            )
        )
        logger.debug("Subscribed to %s", topic)