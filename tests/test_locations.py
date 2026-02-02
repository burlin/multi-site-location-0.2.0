import ftrack_api # type: ignore
# from ftrack_s3_accessor.s3 import S3Accessor # type: ignore
import logging

logger = logging.getLogger(
    'com.ftrack.recipes.pyt_test'
)

EXCLUDED_LOCATIONS = [
    'ftrack.origin',
    'ftrack.connect',
    'ftrack.server',
    'ftrack.unmanaged',
    'ftrack.review',
]

session = ftrack_api.Session(
    server_url='https://mroya-studio.ftrackapp.com/',
    api_key='NzE2Yzk1MjMtOTQ1ZS00NGZkLTlmYTItMGU3OWI2ODU1NTgyOjpkMDgwMGY3MS1jMGZjLTQxZTQtYTA1MC0yNWU1OTU3MGZlZDQ',
    api_user='michael.levi.fx@gmail.com',
)


all_locations = session.query('Location').all()

# for location in all_locations:
#     print(location['name'], location.accessor)
#     if location.accessor and location['name'] not in EXCLUDED_LOCATIONS:
#         print(location['name'])

loc = session.pick_location()
print(loc)