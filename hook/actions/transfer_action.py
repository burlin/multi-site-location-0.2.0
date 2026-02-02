import json
import logging
import threading
import collections
import ftrack_api # type: ignore

SUPPORTED_ENTITY_TYPES = ('assetversion', 'TypedContext', 'Project', 'Component')

def _async(fn):
    '''Run *fn* asynchronously.'''

    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
        thread.start()

    return wrapper

def get_filter_string(entity_ids):
    '''Return a comma separated string of quoted ids from *entity_ids* list.'''
    return ', '.join('"{0}"'.format(entity_id) for entity_id in entity_ids)


class TransferComponentsPlusAction(object):
    '''Transfer components action.'''

    label = 'Transfer Components [new]'
    identifier = 'transfer.components'
    description = 'Transfer components between locations.'

    excluded_locations = [
        'ftrack.origin',
        'ftrack.connect',
        'ftrack.server',
        'ftrack.unmanaged',
        'ftrack.review',
    ]
    
    def __init__(self, session):
        '''Initialise action.'''
        super().__init__()
        self.session = session
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
        )

    def register(self):
        '''Register action.'''
        self.session.event_hub.subscribe(
            'topic=ftrack.action.discover and source.user.username={0}'.format(
            self.session.api_user
            ),
            self.discover
        )
        self.session.event_hub.subscribe(
            'topic=ftrack.action.launch and data.actionIdentifier={0} and '
            'source.user.username={1}'.format(
            self.identifier,
            self.session.api_user
            ),
            self.launch
        )

    def discover(self, event):
        '''Return action config if triggered on a single asset version.'''
        data = event['data']
        '''
        If selection contains more than one item return early since
        this action can only handle a single version.
        '''
        selection = data.get('selection', [])
        self.logger.info('Got selection: {0}'.format(selection))
        if len(selection) != 1 or selection[0]['entityType'] not in SUPPORTED_ENTITY_TYPES:
            self.logger.info(
                'Selection not supported: {0}'.format(selection)
            )
            return
        return {
            'items': [{
            'label': self.label,
            'description': self.description,
            'actionIdentifier': self.identifier
            }]
        }

    
    def interface(self, session, entities, event):
        '''Return interface.'''
        values = event['data'].get('values', {})

        if not values:
            locations = [
                location
                for location in session.query('select name, label from Location').all()
                if location.accessor
            ]
            # Sort by priority.
            locations = sorted(locations, key=lambda location: location.priority)

            # Remove built in locations
            locations = [
                location
                for location in locations
                if location['name'] not in self.excluded_locations
            ]
            self.logger.info(locations)

            locations_options = [
                {
                    'label': location['label'] or location['name'],
                    'value': location['id'],
                }
                for location in locations
            ]
            interface = {
                'items': [
                    {'value': 'Transfer components between locations', 'type': 'label'},
                    {
                        'label': 'From location',
                        'type': 'enumerator',
                        'name': 'from_location',
                        'value': locations_options[0]['value'],
                        'data': locations_options,
                    },
                    {
                        'label': 'To location',
                        'type': 'enumerator',
                        'name': 'to_location',
                        'value': locations_options[1]['value'],
                        'data': locations_options,
                    },
                    {'value': '---', 'type': 'label'},
                    {
                        'label': 'Ignore missing',
                        'type': 'enumerator',
                        'name': 'ignore_component_not_in_location',
                        'value': 'false',
                        'data': [
                            {'label': 'Yes', 'value': 'true'},
                            {'label': 'No', 'value': 'false'},
                        ],
                    },
                    {
                        'label': 'Ignore errors',
                        'type': 'enumerator',
                        'name': 'ignore_location_errors',
                        'value': 'false',
                        'data': [
                            {'label': 'Yes', 'value': 'true'},
                            {'label': 'No', 'value': 'false'},
                        ],
                    },
                ]
            }
            self.logger.info(
                'Returning interface: {0}'.format(interface)
            )
            return interface
    
    def get_components_in_location(self, session, entities, location):
        '''Return list of components in *entities*.'''
        component_queries = []
        entity_groups = collections.defaultdict(list)
        for selection in entities:
            entity_type = selection['entityType']
            entity_id = selection['entityId']
            self.logger.info(
                'Processing entity: {0} ({1})'.format(entity_type, entity_id)
            )
            entity_groups[entity_type].append(entity_id)
        self.logger.info(
            'Entity groups: {0}'.format(entity_groups)
        )
        
        if entity_groups['Project']:
            component_queries.append(
                'Component where (version.asset.parent.project.id in ({0}) or '
                'version.asset.parent.id in ({0}))'.format(
                    get_filter_string(entity_groups['Project'])
                )
            )

        if entity_groups['TypedContext']:
            component_queries.append(
                'Component where (version.asset.parent.ancestors.id in ({0}) or '
                'version.asset.parent.id in ({0}))'.format(
                    get_filter_string(entity_groups['TypedContext'])
                )
            )

        if entity_groups['assetversion']:
            component_queries.append(
                'Component where version_id in ({0})'.format(
                    get_filter_string(entity_groups['assetversion'])
                )
            )

        if entity_groups['Component']:
            component_queries.append(
                'Component where id in ({0})'.format(
                    get_filter_string(entity_groups['Component'])
                )
            )

        components = set()
        for query_string in component_queries:
            self.logger.info(
                'Querying components with: {0}'.format(query_string)
            )
            components.update(
                session.query(
                    '{0} and component_locations.location_id is "{1}"'.format(
                        query_string, location['id']
                    )
                ).all()
            )

        self.logger.info('Found {0} components in selection'.format(len(components)))
        return list(components)
    
    @_async
    def transfer_components(
        self,
        entities,
        source_location,
        target_location,
        user_id=None,
        ignore_component_not_in_location=False,
        ignore_location_errors=False,
    ):
        '''Transfer components in *entities* from *source_location*.

        if *ignore_component_not_in_location*, ignore components missing in
        source location. If *ignore_location_errors* is specified, ignore all
        locations-related errors.

        Reports progress back to *user_id* using a job.

        '''

        session = ftrack_api.Session(auto_connect_event_hub=False)
        job = session.create(
            'Job',
            {
                'user_id': user_id,
                'status': 'running',
                'data': json.dumps(
                    {'description': 'Transfer components (Gathering...)'}
                ),
            },
        )
        self.logger.info(
            'Creating job for user {0}: {1}'.format(
                user_id, job['id']
            )
        )
        session.commit()
        try:
            components = self.get_components_in_location(
                session, entities, source_location
            )
            amount = len(components)
            self.logger.info('>>> Transferring {0} components'.format(amount))

            for index, component in enumerate(components, start=1):
                self.logger.info(
                    'Transferring component ({0} of {1})'.format(index, amount)
                )
                job['data'] = json.dumps(
                    {
                        'description': 'Transfer components ({0} of {1})'.format(
                            index, amount
                        )
                    }
                )
                session.commit()

                try:
                    target_location.add_component(component, source=source_location)
                except ftrack_api.exception.ComponentInLocationError:
                    self.logger.info(
                        'Component ({}) already in target location'.format(component)
                    )
                except ftrack_api.exception.ComponentNotInLocationError:
                    if ignore_component_not_in_location or ignore_location_errors:
                        self.logger.exception('Failed to add component to location')
                    else:
                        raise
                except ftrack_api.exception.LocationError:
                    if ignore_location_errors:
                        self.logger.exception('Failed to add component to location')
                    else:
                        raise

            job['status'] = 'done'
            session.commit()

            self.logger.info('Transfer complete ({0} components)'.format(amount))

        except BaseException:
            self.logger.exception('Transfer failed')
            session.rollback()
            job['status'] = 'failed'
            session.commit()
            
    def launch(self, event):
        # self.logger.info('>>> Launching transfer action with args: {0}, kwargs: {1}'.format(args, kwargs))
        self.logger.info(event) 
        values = event['data'].get('values', {})
        if 'values' in event['data']:
            values = event['data']['values']
            self.logger.info(u'Got values: {0}'.format(values))
            # return {
            #     'success': True,
            #     'message': 'Ran my custom action successfully!'
            # }
            source_location = self.session.get('Location', values['from_location'])
            target_location = self.session.get('Location', values['to_location'])
            if source_location == target_location:
                return {
                    'success': False,
                    'message': 'Source and target locations are the same.',
                }

            ignore_component_not_in_location = (
                values.get('ignore_component_not_in_location') == 'true'
            )
            ignore_location_errors = values.get('ignore_location_errors') == 'true'

            self.logger.info(
                'Transferring components from {0} to {1}'.format(
                    source_location, target_location
                )
            )
            user_id = event['source']['user']['id']
            entities = event['data']['selection']
            self.transfer_components(
                entities,
                source_location,
                target_location,
                user_id=user_id,
                ignore_component_not_in_location=ignore_component_not_in_location,
                ignore_location_errors=ignore_location_errors,
            )
            return {'success': True, 'message': 'Transferring components...'}


        return self.interface(
            self.session, 
            event['data'].get('entities', []), 
            event
        )
        

def register(session, **kw):
    '''Register plugin.'''
    if not isinstance(session, ftrack_api.Session):
        return

    action = TransferComponentsPlusAction(session)
    action.register()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    session = ftrack_api.Session()
    register(session)
    session.event_hub.wait()