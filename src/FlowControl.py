import web, json, logging, datetime
import SprinklerDB
from SprinklerConfig import config

logger = logging.getLogger(__name__)


# Handles all flow sensor related requests
class FlowRequestHandler(object):
    def __init__(self):
        # Authenticate a request and save the associated product and component ID
        logger.debug('Authenticating a new request')
        self.product_id = SprinklerDB.authorize_product()
        if not self.product_id:
            logger.warn('Not Authenticated: Unknown API KEY')
            raise web.unauthorized('Unknown API KEY')
        self.flow_id = SprinklerDB.get_component_id(self.product_id, config['FLOW_COMP_TYPE'])
        if not self.flow_id:
            logger.warn('Not Authenticated: Not authorized for component')
            raise web.unauthorized('Not authorized for component')
        logger.info('Request authenticated: Product ID:%d, Valve ID:%d', self.product_id, self.flow_id)

    # Saves the flow sensor info
    def POST(self):
        try:
            logger.debug('Request description: POST request for saving flow sensor data')
            flow_info = json.loads(web.data())
            logger.info('Data to save: %s', flow_info)
            insert_flow_info(self.flow_id, flow_info)
            logger.info('Request completed successfully')
        except ValueError as ex:
            logger.exception("Could not save data as it is invalid")
            raise web.BadRequest(ex.message)
        except Exception as ex:
            logger.exception("Could not save data due to unknown error")
            raise web.InternalError(ex.message)


# Saves the flow data
# JSON shape {'volume': <float>, duraion: <float>}
def insert_flow_info(flow_id, flow_info):
    logger.debug('Inserting flow data for Flow Sensor ID %d - %s', flow_id, flow_info)
    try:
        volume = flow_info['volume']
        duration = flow_info['duration']
        assert volume is not None and type(volume) in [int, float] and volume >= 0
        assert duration is not None and type(duration) in [int, float] and duration >= 0
    except Exception as ex:
        logger.exception('Invalid flow data')
        raise ValueError('Invalid flow data')
    with SprinklerDB.Connection() as cursor:
        fetch_last_created_sql = 'SELECT created FROM flow_rate WHERE id = ' + \
                                 '(SELECT max(id) FROM flow_rate WHERE component_id=%s)'
        count = cursor.execute(fetch_last_created_sql, (flow_id,))
        if count > 0:
            row = cursor.fetchone()
            last_created = row['created']
        else:
            last_created = None
        if last_created is not None:
            now = datetime.datetime.now()
            duration = (now - last_created).total_seconds()
            logger.debug('Duration has been updated from %s to %s', flow_info['duration'], duration)
        sql = 'INSERT INTO flow_rate (component_id, flow_volume, flow_duration) VALUES (%s, %s, %s)'
        inserted = cursor.execute(sql, (flow_id, volume, duration))
        logger.debug('Rows inserted: %s', inserted)
    logger.debug('Inserted flow sensor data for Flow Sensor ID %d', flow_id)
