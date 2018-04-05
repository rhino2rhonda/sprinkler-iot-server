import web, json
import logging, datetime
import SprinklerDB
from SprinklerConfig import config

logger = logging.getLogger(__name__)


# Handles all flow sensor related requests
class ValveRequestHandler(object):
    def __init__(self):
        # Authenticate a request and save the associated product and component ID
        logger.debug('Authenticating a new request')
        self.product_id = SprinklerDB.authorize_product()
        if not self.product_id:
            logger.warn('Not Authenticated: Unknown API KEY')
            raise web.unauthorized('Unknown API KEY')
        self.valve_id = SprinklerDB.get_component_id(self.product_id, config['VALVE_COMP_TYPE'])
        if not self.valve_id:
            logger.warn('Not Authenticated: Not authorized for component')
            raise web.unauthorized('Not authorized for component')
        logger.info('Request authenticated: Product ID:%d, Valve ID:%d', self.product_id, self.valve_id)

    # Gets the current state of the valve with the considering all the valve controllers
    # Saves the product heart beat
    def GET(self):
        try:
            logger.debug('Request description: GET request for fetching valve data')
            SprinklerDB.send_heart_beat(self.product_id)
            valve_info = get_valve_info(self.valve_id)
            web.header('Content-Type', 'application/json')
            logger.info('Request completed. Sending data: %s', valve_info)
            return json.dumps(valve_info)
        except Exception as ex:
            logger.exception("Could not fetch data due to unknown error")
            raise web.InternalError(ex.message)

    # Saves the valve info
    def POST(self):
        try:
            logger.debug('Request description: POST request for saving valve data')
            valve_info = json.loads(web.data())
            logger.info('Data to save: %s', valve_info)
            update_valve_info(self.valve_id, valve_info)
            logger.info('Request completed successfully')
        except ValueError as ex:
            logger.exception("Could not save data as it is invalid")
            raise web.BadRequest(ex.message)
        except Exception as ex:
            logger.exception("Could not save data due to unknown error")
            raise web.InternalError(ex.message)


# Fetches the details of the latest remote switch job
def remote_valve_controller(valve_id):
    with SprinklerDB.Connection() as cursor:
        sql = 'SELECT id, state, created, completion_status FROM valve_remote_switch_job WHERE id=(SELECT max(id) ' + \
              'FROM valve_remote_switch_job WHERE component_id=%s)'
        count = cursor.execute(sql, (valve_id,))
        # Defaults
        info = {
            'id': None,
            'completed': False,
            'controller': config['VALVE_CONTROLLER_REMOTE'],
            'state': 0,
            'due': None,
            'forced': False
        }
        if count <= 0:
            logger.warn('No remote switch jobs found. Using defaults')
            return info

        row = cursor.fetchone()

        job_id = row['id']
        if job_id is None:
            logger.error('Invalid Job ID %s for latest remote switch job. Reverting to defaults', job_id)
            return info

        state = row['state']
        if state is None or state not in [0, 1]:
            logger.error('Invalid state %s for latest remote switch job. Reverting to defaults', state)
            return info

        due = row['created']
        if due is None:
            logger.error('Invalid creation time %s for latest remote switch job. Reverting to defaults', due)
            return info

        info.update({
            'id': job_id,
            'state': state,
            'due': due,
            'completed': row['completion_status'] == 1,
            'forced': True
        })
        return info


# Fetches the details of the latest valve timer
def valve_timer_controller(valve_id):
    with SprinklerDB.Connection() as cursor:
        sql = "SELECT id, enabled, start_time, end_time FROM valve_timer WHERE " + \
              "id=(SELECT max(id) FROM valve_timer WHERE component_id=%s)"
        count = cursor.execute(sql, (valve_id,))
        # Defaults
        info = {
            'id': None,
            'controller': config['VALVE_CONTROLLER_TIMER'],
            'state': 0,
            'due': None,
            'forced': False
        }
        if count <= 0:
            logger.warn('No valve timers found. Using defaults')
            return info

        row = cursor.fetchone()

        if row['enabled'] is None or row['enabled'] not in [0, 1]:
            logger.error('Invalid value for enabled %s for valve timer. Reverting to defaults', row['enabled'])
            return info
        timer_enabled = row['enabled'] == 1
        if not timer_enabled:
            logger.debug('Valve timer is disabled. Using defaults')
            return info

        start_time = row['start_time']
        if start_time is None \
                or type(start_time) is not datetime.timedelta \
                or not 0 <= start_time.total_seconds() <= 24 * 60 * 60:
            logger.error("Invalid value for timer start time %s for valve timer. Reverting to defaults", start_time)
            return info

        end_time = row['end_time']
        if end_time is None \
                or type(end_time) is not datetime.timedelta \
                or not 0 <= end_time.total_seconds() <= 24 * 60 * 60:
            logger.error("Invalid value for timer end time %s for valve timer. Reverting to defaults", end_time)
            return info

        if start_time.total_seconds() > end_time.total_seconds():
            logger.error(
                "Invalid values: Start time %s is greater than end time %s for valve timer. Reverting to defaults",
                start_time, end_time)
            return info

        curr_time = datetime.datetime.now()
        today_begin = datetime.datetime.combine(curr_time.date(), datetime.time.min)
        timer_start = today_begin + start_time
        timer_end = today_begin + end_time
        info['state'] = 1 if timer_start <= curr_time < timer_end else 0
        info['due'] = today_begin if curr_time < timer_start else (timer_start if curr_time < timer_end else timer_end)
        logger.debug("Valve timer is enabled. Current time lies in timer duration?: %d", info['state'])
        return info


# Aggregates all the controllers details and returns the final state of the valve
# JSON shape {'state': <0 or 1>, id: <Job ID>}
def get_valve_info(valve_id):
    logger.debug('Fetching aggregated valve info for Valve ID: %d', valve_id)
    remote_info = remote_valve_controller(valve_id)
    logger.debug('Remote info fetched: %s', remote_info)
    timer_info = valve_timer_controller(valve_id)
    logger.debug('Timer info fetched: %s', timer_info)
    valve_info = remote_info
    if timer_info['state'] == 1:
        if remote_info['state'] == 0 and (remote_info['due'] is None or timer_info['due'] > remote_info['due']):
            valve_info = timer_info
    logger.debug('Fetched aggregated valve info for Valve ID: %d - %s', valve_id, valve_info)
    return {k: v for k, v in valve_info.iteritems() if k in ['id', 'state']}


# Marks a remote switch job as completed
def mark_remote_job_completed(job_id):
    logger.info('Marking remote switch job %d as completed', job_id)
    with SprinklerDB.Connection() as cursor:
        sql = 'UPDATE valve_remote_switch_job SET completion_status=1 WHERE id=%s'
        updated = cursor.execute(sql, (job_id,))
        logger.debug('Rows updated: %s', updated)
    logger.info('Marked remote switch job %d as completed', job_id)


# Saves the valve state
# Runs callbacks on successful save
# JSON shape {'state': <0 or 1>, id: <Job ID>}
def update_valve_info(valve_id, valve_info):
    logger.debug('Updating valve state for Valve ID %d - %s', valve_id, valve_info)
    job_id = valve_info['id'] if valve_info.has_key('id') else None
    state = valve_info['state']
    if state not in [0, 1]:
        logger.error("Invalid valve state %s", state)
        raise ValueError("Invalid valve state %s" % state)
    with SprinklerDB.Connection() as cursor:
        sql = 'INSERT INTO valve_state (component_id, state) VALUES (%s, %s) ON DUPLICATE KEY UPDATE state=%s'
        updated = cursor.execute(sql, (valve_id, state, state))
        logger.debug('Rows updated: %s', updated)
        if job_id is not None:
            mark_remote_job_completed(job_id)
    logger.debug('Updated valve state for Valve ID %d', valve_id)
