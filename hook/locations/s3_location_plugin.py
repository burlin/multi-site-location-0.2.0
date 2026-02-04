import ftrack_api # type: ignore
import logging
import os
import sys
import socket
import functools
from pathlib import Path
from typing import Any
from types import MethodType

logger = logging.getLogger("ftrack_connect.multi_site_location.s3_location_plugin")

# Load dependencies
plugin_path = Path(__file__).parent.parent.parent
dependencies_path = plugin_path / 'dependencies'
if dependencies_path.exists() and str(dependencies_path) not in sys.path:
    sys.path.insert(0, str(dependencies_path))

import boto3 # type: ignore # noqa: F401, E402
from botocore.config import Config # type: ignore # noqa: F401, E402
from ftrack_s3_accessor.s3 import S3Accessor # type: ignore # noqa: F401, E402
import dotenv # type: ignore # noqa: F401, E402
dotenv.load_dotenv(plugin_path / '.env')


def _get_s3_api_endpoint() -> str | None:
    """Return S3 API endpoint, supporting both old and new env var names.

    Prefer S3_MINIO_ENDPOINT_URL (current name), but fall back to
    S3_MINIO_API_ENDPOINT_URL if needed to maintain backward compatibility.
    """
    value = os.getenv("S3_MINIO_ENDPOINT_URL")
    if value:
        return value
    return os.getenv("S3_MINIO_API_ENDPOINT_URL")


def get_url_patch(self, resource_identifier=None):
    """Generate a presigned URL for the given resource identifier.
    
    This method is a patch for the S3Accessor's get_url method to use
    boto3's presigned URL generation instead of the default implementation.
    """
    # logger.info('>>> get_url_patch called with resource_identifier:', resource_identifier)
    s3_client = boto3.client(
        "s3",
        endpoint_url=_get_s3_api_endpoint(),
        config=Config(signature_version="s3v4"),
        # Enable secure connection to S3/MinIO.
        use_ssl=True,
        verify=True,
    )
    url = s3_client.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': 'proj', 
            'Key': resource_identifier
        },
        ExpiresIn=3600 
    )
    logger.info(" >>> get_url", url)
    return url

def session_add_s3_location(session: ftrack_api.Session) -> None:
    """Add S3 location to the session."""
    location_name = (os.getenv('S3_LOCATION_NAME') or '').strip()
    if not location_name:
        logger.warning(
            "S3_LOCATION_NAME is not set or empty; skipping S3 location "
            "(avoids NullConstraintError/IntegrityError on server)."
        )
        return

    # S3 bucket must be configured
    bucket = (os.getenv("S3_BUCKET") or "").strip()
    if not bucket:
        logger.warning(
            "S3_BUCKET is not set or empty; skipping S3 location "
            "(no valid storage bucket configured)."
        )
        return

    # Endpoint must be configured (MinIO / S3 API endpoint)
    endpoint = _get_s3_api_endpoint()
    if not endpoint:
        logger.warning(
            "S3_MINIO_ENDPOINT_URL / S3_MINIO_API_ENDPOINT_URL is not set; "
            "skipping S3 location (no API endpoint)."
        )
        return

    # If вообще нет никаких AWS/MinIO кредов — лучше не регистрировать Location,
    # чтобы не плодить нерабочие локации в UI/сервере.
    has_key = bool(os.getenv("AWS_ACCESS_KEY_ID"))
    has_profile = bool(os.getenv("AWS_PROFILE"))
    if not (has_key or has_profile):
        logger.warning(
            "No AWS credentials detected (AWS_ACCESS_KEY_ID / AWS_PROFILE not set); "
            "skipping S3 location registration."
        )
        return
    s3_location = session.ensure(
        'Location', {
            'name': location_name
        }
    )
    s3_location.structure = ftrack_api.structure.standard.StandardStructure()
    s3_accessor = S3Accessor(bucket)
    s3_accessor.get_url = MethodType(get_url_patch, s3_accessor)
    
    # Override the s3 property by directly setting the _s3 attribute
    # This way the original property getter will return our custom boto3 resource
    s3_accessor._s3 = boto3.resource(
        "s3",
        endpoint_url=endpoint,
        # Enable secure connection.
        use_ssl=True,
        verify=True,
    )
    
    s3_location.accessor = s3_accessor
    s3_location.priority = 10
    logger.info(
        "Registered S3 location %s, pointing to: %s",
        location_name,
        endpoint,
    )
            
        
def configure_s3_location(event: ftrack_api.event.base.Event) -> None:
    """Configure the S3 location.
    
    Filters by hostname to prevent registering S3 location from other workstations
    where the same user is logged in.
    """
    # Filter by hostname to prevent registering locations from other workstations
    try:
        current_hostname = socket.gethostname().lower()
    except Exception:
        current_hostname = os.environ.get('COMPUTERNAME', os.environ.get('HOSTNAME', 'unknown')).lower()
    
    event_source = event.get('source', {})
    event_hostname = event_source.get('hostname', '').lower()
    
    # If hostname is set in event source, only process if it matches current hostname
    if event_hostname and event_hostname != current_hostname:
        logger.debug(
            "Skipping S3 location configuration: event from hostname '%s', "
            "current hostname: '%s'",
            event_hostname,
            current_hostname
        )
        return
    
    logger.info(
        "Configuring S3 location for hostname '%s' (event hostname: '%s')",
        current_hostname,
        event_hostname if event_hostname else 'not set'
    )
    
    session = event['data']['session']
    session_add_s3_location(session)
    
def register(api_object: ftrack_api.Session, **kw: Any) -> None:
    """Register the S3 location plugin."""
    logger.info("========= Registering S3 location plugin ==========")
    if not isinstance(api_object, ftrack_api.Session):
        return
    # Subscribe to the event hub
    api_object.event_hub.subscribe(
        'topic=ftrack.api.session.configure-location',
        functools.partial(
            configure_s3_location
        ),
        priority=10
    )