import unittest
import requests
from requests.auth import HTTPBasicAuth
import random, time, datetime, threading, pymysql, logging

import SprinklerDB as DB
import SprinklerServer
import SprinklerLogging
from SprinklerConfig import config
import ValveControl as VC
import FlowControl as FC

logger = logging.getLogger(__name__)

TEST_SERVER_PORT = config['SERVER_PORT'] + 100


def start_server_daemon():
    server_thread = threading.Thread(target=SprinklerServer.start_server, args=(TEST_SERVER_PORT,),
                                     name='TestServerThread')
    server_thread.setDaemon(True)
    server_thread.start()


SprinklerLogging.configure_logging()
start_server_daemon()


# Tests DB connection manager and its properties
class DBConnectionManagerTest(unittest.TestCase):
    def setUp(self):
        self.product_id = 2
        self.invalid_product_id = -2

    @staticmethod
    def insert_heart_beat(cursor, product_id):
        sql = 'INSERT INTO product_heart_beat (product_id) VALUES (%s)'
        cursor.execute(sql, (product_id,))

    def test_1_singularity(self):
        self.assertIs(DB.Connection(), DB.Connection())

    def test_2_auto_commit(self):
        DBUtilsTest.remove_heart_beats(self.product_id)
        count = DBUtilsTest.get_product_heart_beat_count(self.product_id)
        self.assertEqual(count, 0)
        with DB.Connection() as cursor:
            DBConnectionManagerTest.insert_heart_beat(cursor, self.product_id)
        new_count = DBUtilsTest.get_product_heart_beat_count(self.product_id)
        self.assertEqual(new_count, 1)

    def test_3_nested_query_cursor_remains_open(self):
        DBUtilsTest.remove_heart_beats(self.product_id)
        count = DBUtilsTest.get_product_heart_beat_count(self.product_id)
        self.assertEqual(count, 0)
        with DB.Connection() as cursor:
            DBConnectionManagerTest.insert_heart_beat(cursor, self.product_id)
            with DB.Connection() as cursor_nested:
                DBConnectionManagerTest.insert_heart_beat(cursor_nested, self.product_id)
            DBConnectionManagerTest.insert_heart_beat(cursor, self.product_id)
        new_count = DBUtilsTest.get_product_heart_beat_count(self.product_id)
        self.assertEqual(new_count, 3)

    def test_4_auto_close_cursor(self):
        with DB.Connection() as cursor:
            DBConnectionManagerTest.insert_heart_beat(cursor, self.product_id)
        self.assertRaises(pymysql.ProgrammingError, DBConnectionManagerTest.insert_heart_beat, cursor, self.product_id)

    def test_5_rollback_query_error(self):
        count = DBUtilsTest.get_product_heart_beat_count(self.invalid_product_id)
        self.assertEqual(count, 0)

        def raises_error():
            with DB.Connection() as cursor:
                DBConnectionManagerTest.insert_heart_beat(cursor, self.invalid_product_id)

        self.assertRaises(pymysql.IntegrityError, raises_error)
        new_count = DBUtilsTest.get_product_heart_beat_count(self.invalid_product_id)
        self.assertEqual(new_count, 0)

    def test_6_rollback_other_error_same_with_block(self):
        DBUtilsTest.remove_heart_beats(self.product_id)
        count = DBUtilsTest.get_product_heart_beat_count(self.product_id)
        self.assertEqual(count, 0)

        def raises_error():
            with DB.Connection() as cursor:
                DBConnectionManagerTest.insert_heart_beat(cursor, self.product_id)
                raise ValueError()

        self.assertRaises(ValueError, raises_error)
        new_count = DBUtilsTest.get_product_heart_beat_count(self.product_id)
        self.assertEqual(new_count, 0)

    def test_7_rollback_other_error_inside_nested_with_block(self):
        DBUtilsTest.remove_heart_beats(self.product_id)
        count = DBUtilsTest.get_product_heart_beat_count(self.product_id)
        self.assertEqual(count, 0)

        def raises_error():
            with DB.Connection() as cursor:
                DBConnectionManagerTest.insert_heart_beat(cursor, self.product_id)
                with DB.Connection() as nested_cursor:
                    DBConnectionManagerTest.insert_heart_beat(nested_cursor, self.product_id)
                    raise ValueError()

        self.assertRaises(ValueError, raises_error)
        new_count = DBUtilsTest.get_product_heart_beat_count(self.product_id)
        self.assertEqual(new_count, 0)

    def test_8_rollback_other_error_outside_nested_with_block(self):
        DBUtilsTest.remove_heart_beats(self.product_id)
        count = DBUtilsTest.get_product_heart_beat_count(self.product_id)
        self.assertEqual(count, 0)

        def raises_error():
            with DB.Connection() as cursor:
                DBConnectionManagerTest.insert_heart_beat(cursor, self.product_id)
                with DB.Connection() as nested_cursor:
                    DBConnectionManagerTest.insert_heart_beat(nested_cursor, self.product_id)
                raise ValueError()

        self.assertRaises(ValueError, raises_error)
        new_count = DBUtilsTest.get_product_heart_beat_count(self.product_id)
        self.assertEqual(new_count, 0)

    def test_9_synchronised_access(self):
        pass_ = True
        shared = {"counter": 0}
        num_threads = 20
        sleep_time = 0.2

        def increment(shared):
            with DB.Connection() as cursor:
                if shared["counter"] is not 0:
                    pass_ = False
                shared["counter"] += 1
                time.sleep(sleep_time)
                shared["counter"] -= 1

        threads = []
        for x in range(num_threads):
            thrd = threading.Thread(target=increment, args=(shared,))
            thrd.start()
            threads.append(thrd)
        for t in threads:
            t.join()
        self.assertIs(shared["counter"], 0)
        self.assertTrue(pass_)

    @unittest.skip('')
    def test_10_interactive_connection_revival(self):
        def test_connection():
            with DB.Connection() as cursor:
                sql = "select 1"
                cursor.execute(sql)

        test_connection()
        raw_input("Disable DB connection and press ENTER")
        self.assertRaises(pymysql.OperationalError, test_connection)
        revived = DB.Connection().revive_connection()
        self.assertFalse(revived)
        raw_input("Enable DB connection and press ENTER")
        revived = DB.Connection().revive_connection()
        self.assertTrue(revived)
        test_connection()


# Tests utility methods in SprinklerDB
class DBUtilsTest(unittest.TestCase):
    def setUp(self):
        self.product_key = '$(2#2Da$131s&*f4!x'
        self.product_id = 2
        self.valve_id = 3
        self.flow_id = 4

    @staticmethod
    def remove_heart_beats(product_id):
        with DB.Connection() as cursor:
            sql = 'DELETE FROM product_heart_beat WHERE product_id=%s'
            cursor.execute(sql, (product_id,))

    @staticmethod
    def get_product_heart_beat_count(product_id):
        with DB.Connection() as cursor:
            sql = 'SELECT 1 FROM product_heart_beat WHERE product_id=%s'
            count = cursor.execute(sql, (product_id,))
            return count

    def test_1_product_id(self):
        product_id = DB.get_product_id(self.product_key)
        self.assertIs(product_id, self.product_id)

    def test_2_valve_id(self):
        valve_id = DB.get_component_id(self.product_id, config['VALVE_COMP_TYPE'])
        self.assertIs(valve_id, self.valve_id)

    def test_3_flow_id(self):
        flow_id = DB.get_component_id(self.product_id, config['FLOW_COMP_TYPE'])
        self.assertIs(flow_id, self.flow_id)

    def test_4_heart_beat(self):
        DBUtilsTest.remove_heart_beats(self.product_id)
        DB.send_heart_beat(self.product_id)
        count = DBUtilsTest.get_product_heart_beat_count(self.product_id)
        self.assertEqual(count, 1)


# Tests basic auth of client requests
class ServerAuthTest(unittest.TestCase):
    def setUp(self):
        self.api_url = 'http://localhost:' + str(TEST_SERVER_PORT)
        self.product_key = '$(2#2Da$131s&*f4!x'
        self.invalid_product_key = 'invalid_key'

    def test_1_valid_key_valve(self):
        resp = requests.get(self.api_url + '/valve', auth=HTTPBasicAuth('API_KEY', self.product_key))
        self.assertEqual(resp.status_code, 200)

    def test_2_valid_key_flow(self):
        resp = requests.post(self.api_url + '/flow', auth=HTTPBasicAuth('API_KEY', self.product_key),
                             json={'volume': 10, 'duration': 20})
        self.assertEqual(resp.status_code, 200)

    def test_3_invalid_key_valve(self):
        resp = requests.get(self.api_url + '/valve', auth=HTTPBasicAuth('API_KEY', self.invalid_product_key))
        self.assertEqual(resp.status_code, 401)

    def test_4_invalid_key_flow(self):
        resp = requests.post(self.api_url + '/flow', auth=HTTPBasicAuth('API_KEY', self.invalid_product_key))
        self.assertEqual(resp.status_code, 401)


# Tests valve remote job controller
class ValveRemoteJobControllerTest(unittest.TestCase):
    def setUp(self):
        self.valve_id = 3

    @staticmethod
    def add_job(valve_id, state, created=None):
        with DB.Connection() as cursor:
            if created:
                sql = 'INSERT INTO valve_remote_switch_job (component_id, state, created) VALUES (%s, %s, %s)'
                inserted = cursor.execute(sql, (valve_id, state, created))
                assert inserted == 1
            else:
                sql = 'INSERT INTO valve_remote_switch_job (component_id, state) VALUES (%s, %s)'
                inserted = cursor.execute(sql, (valve_id, state))
                assert inserted == 1

    @staticmethod
    def remove_all_jobs(valve_id):
        with DB.Connection() as cursor:
            sql = 'DELETE FROM valve_remote_switch_job WHERE component_id=%s'
            cursor.execute(sql, (valve_id,))

    def test_1_fetch_no_jobs(self):
        ValveRemoteJobControllerTest.remove_all_jobs(self.valve_id)
        expected_valve_info = {
            'id': None,
            'controller': config['VALVE_CONTROLLER_REMOTE'],
            'state': 0,
            'due': None,
            'completed': False,
            'forced': False
        }
        valve_info = VC.remote_valve_controller(self.valve_id)
        self.assertEqual(valve_info, expected_valve_info)

    def test_2_fetch_open(self):
        ValveRemoteJobControllerTest.add_job(self.valve_id, 1)
        valve_info = VC.remote_valve_controller(self.valve_id)
        self.assertEqual(valve_info['controller'], config['VALVE_CONTROLLER_REMOTE'])
        self.assertFalse(valve_info['completed'])
        self.assertEqual(valve_info['state'], 1)
        self.assertIsNotNone(valve_info['id'])
        self.assertIsNotNone(valve_info['due'])
        self.assertTrue(valve_info['forced'])

    def test_3_fetch_close(self):
        ValveRemoteJobControllerTest.add_job(self.valve_id, 0)
        valve_info = VC.remote_valve_controller(self.valve_id)
        self.assertEqual(valve_info['controller'], config['VALVE_CONTROLLER_REMOTE'])
        self.assertFalse(valve_info['completed'])
        self.assertEqual(valve_info['state'], 0)
        self.assertIsNotNone(valve_info['id'])
        self.assertIsNotNone(valve_info['due'])
        self.assertTrue(valve_info['forced'])

    def test_4_fetch_invalid_state(self):
        ValveRemoteJobControllerTest.add_job(self.valve_id, -1)
        expected_valve_info = {
            'id': None,
            'controller': config['VALVE_CONTROLLER_REMOTE'],
            'state': 0,
            'due': None,
            'completed': False,
            'forced': False
        }
        valve_info = VC.remote_valve_controller(self.valve_id)
        self.assertEqual(valve_info, expected_valve_info)

    def test_5_fetch_many(self):
        num_iters = 10
        for x in range(num_iters):
            curr_state = random.randint(0, 1)
            ValveRemoteJobControllerTest.add_job(self.valve_id, curr_state)
            valve_info = VC.remote_valve_controller(self.valve_id)
            self.assertEqual(valve_info['controller'], config['VALVE_CONTROLLER_REMOTE'])
            self.assertFalse(valve_info['completed'])
            self.assertEqual(valve_info['state'], curr_state)
            self.assertIsNotNone(valve_info['id'])
            self.assertIsNotNone(valve_info['due'])
            self.assertTrue(valve_info['forced'])

    def test_6_update_job_open(self):
        ValveRemoteJobControllerTest.add_job(self.valve_id, 1)
        valve_info = VC.remote_valve_controller(self.valve_id)
        self.assertFalse(valve_info['completed'])
        self.assertIsNotNone(valve_info['id'])
        job_id = valve_info['id']
        VC.mark_remote_job_completed(job_id)
        valve_info = VC.remote_valve_controller(self.valve_id)
        self.assertTrue(valve_info['completed'])
        self.assertEqual(valve_info['id'], job_id)


# Tests valve timer controller
class ValveTimerControllerTest(unittest.TestCase):
    def setUp(self):
        self.valve_id = 3

    @staticmethod
    def replace_timer(valve_id, enabled, start_time=None, end_time=None):
        with DB.Connection() as cursor:
            sql = "INSERT INTO valve_timer (component_id, enabled, start_time, end_time) " + \
                  "VALUES(%s, %s, %s, %s) ON DUPLICATE KEY UPDATE id=id, enabled=%s, start_time=%s, end_time=%s"
            updated = cursor.execute(sql, (valve_id, enabled, start_time, end_time, enabled, start_time, end_time))

    @staticmethod
    def remove_all_timers(valve_id):
        with DB.Connection() as cursor:
            sql = "DELETE FROM valve_timer WHERE component_id=%s"
            cursor.execute(sql, (valve_id,))

    @staticmethod
    def get_curr_time():
        curr_time = datetime.datetime.now().time()
        return datetime.timedelta(hours=curr_time.hour, minutes=curr_time.minute, seconds=curr_time.second)

    def assert_default_timer_info(self, timer_info):
        expected_info = {
            'id': None,
            'controller': config['VALVE_CONTROLLER_TIMER'],
            'state': 0,
            'due': None,
            'forced': False
        }
        self.assertEqual(timer_info, expected_info)

    def test_1_fetch_no_timers(self):
        ValveTimerControllerTest.remove_all_timers(self.valve_id)
        info = VC.valve_timer_controller(self.valve_id)
        self.assert_default_timer_info(info)

    def test_2_fetch_timer_disabled(self):
        ValveTimerControllerTest.replace_timer(self.valve_id, 0)
        info = VC.valve_timer_controller(self.valve_id)
        self.assert_default_timer_info(info)

    def test_3_fetch_timer_invalid_enabled_value(self):
        curr_time = ValveTimerControllerTest.get_curr_time()
        test_start_time = curr_time - datetime.timedelta(minutes=10)
        test_end_time = curr_time + datetime.timedelta(minutes=10)
        ValveTimerControllerTest.replace_timer(self.valve_id, -1, test_start_time, test_end_time)
        info = VC.valve_timer_controller(self.valve_id)
        self.assert_default_timer_info(info)

    def test_4_fetch_timer_invalid_start_time_empty(self):
        curr_time = ValveTimerControllerTest.get_curr_time()
        test_start_time = None
        test_end_time = curr_time - datetime.timedelta(minutes=10)
        ValveTimerControllerTest.replace_timer(self.valve_id, 1, test_start_time, test_end_time)
        info = VC.valve_timer_controller(self.valve_id)
        self.assert_default_timer_info(info)

    def test_5_fetch_timer_invalid_start_time_outside_day(self):
        curr_time = ValveTimerControllerTest.get_curr_time()
        test_start_time = curr_time + datetime.timedelta(minutes=60 * 25)
        test_end_time = curr_time + datetime.timedelta(minutes=10)
        ValveTimerControllerTest.replace_timer(self.valve_id, 1, test_start_time, test_end_time)
        info = VC.valve_timer_controller(self.valve_id)
        self.assert_default_timer_info(info)

    def test_6_fetch_timer_invalid_end_time_empty(self):
        curr_time = ValveTimerControllerTest.get_curr_time()
        test_start_time = curr_time - datetime.timedelta(minutes=20)
        test_end_time = None
        ValveTimerControllerTest.replace_timer(self.valve_id, 1, test_start_time, test_end_time)
        info = VC.valve_timer_controller(self.valve_id)
        self.assert_default_timer_info(info)

    def test_7_fetch_timer_invalid_end_time_outside_day(self):
        curr_time = ValveTimerControllerTest.get_curr_time()
        test_start_time = curr_time + datetime.timedelta(minutes=20)
        test_end_time = curr_time + datetime.timedelta(minutes=60 * 25)
        ValveTimerControllerTest.replace_timer(self.valve_id, 1, test_start_time, test_end_time)
        info = VC.valve_timer_controller(self.valve_id)
        self.assert_default_timer_info(info)

    def test_8_fetch_timer_invalid_start_time_before_end_time(self):
        curr_time = ValveTimerControllerTest.get_curr_time()
        test_start_time = curr_time + datetime.timedelta(minutes=10)
        test_end_time = curr_time - datetime.timedelta(minutes=10)
        ValveTimerControllerTest.replace_timer(self.valve_id, 1, test_start_time, test_end_time)
        info = VC.valve_timer_controller(self.valve_id)
        self.assert_default_timer_info(info)

    def test_9_fetch_timer_enabled_past(self):
        curr_time = ValveTimerControllerTest.get_curr_time()
        test_start_time = curr_time - datetime.timedelta(minutes=20)
        test_end_time = curr_time - datetime.timedelta(minutes=10)
        ValveTimerControllerTest.replace_timer(self.valve_id, 1, test_start_time, test_end_time)
        info = VC.valve_timer_controller(self.valve_id)
        self.assertEqual(info['controller'], config['VALVE_CONTROLLER_TIMER'])
        self.assertEqual(info['state'], 0)
        self.assertIsNone(info['id'])
        self.assertFalse(info['forced'])
        self.assertIsNotNone(info['due'])
        self.assertLess(info['due'], datetime.datetime.now())

    def test_10_fetch_timer_enabled_future(self):
        curr_time = self.get_curr_time()
        test_start_time = curr_time + datetime.timedelta(minutes=10)
        test_end_time = curr_time + datetime.timedelta(minutes=20)
        ValveTimerControllerTest.replace_timer(self.valve_id, 1, test_start_time, test_end_time)
        info = VC.valve_timer_controller(self.valve_id)
        self.assertEqual(info['controller'], config['VALVE_CONTROLLER_TIMER'])
        self.assertEqual(info['state'], 0)
        self.assertIsNone(info['id'])
        self.assertFalse(info['forced'])
        self.assertIsNotNone(info['due'])
        self.assertLess(info['due'], datetime.datetime.now())

    def test_11_fetch_timer_enabled_now(self):
        curr_time = self.get_curr_time()
        test_start_time = curr_time - datetime.timedelta(minutes=10)
        test_end_time = curr_time + datetime.timedelta(minutes=10)
        ValveTimerControllerTest.replace_timer(self.valve_id, 1, test_start_time, test_end_time)
        info = VC.valve_timer_controller(self.valve_id)
        self.assertEqual(info['controller'], config['VALVE_CONTROLLER_TIMER'])
        self.assertEqual(info['state'], 1)
        self.assertIsNone(info['id'])
        self.assertFalse(info['forced'])
        self.assertIsNotNone(info['due'])
        self.assertLess(info['due'], datetime.datetime.now())


# Tests aggregated valve info fetch method
class ValveInfoFetchTest(unittest.TestCase):
    def setUp(self):
        self.valve_id = 3

    @staticmethod
    def configure_timer(valve_id, state):
        if state == 'empty':
            ValveTimerControllerTest.remove_all_timers(valve_id)
        elif state == 'off':
            ValveTimerControllerTest.replace_timer(valve_id, 0)
        elif 'enabled' in state:
            curr_time = ValveTimerControllerTest.get_curr_time()
            if state == 'enabled_past':
                test_start_time = curr_time - datetime.timedelta(minutes=20)
                test_end_time = curr_time - datetime.timedelta(minutes=10)
            elif state == 'enabled_current':
                test_start_time = curr_time - datetime.timedelta(minutes=10)
                test_end_time = curr_time + datetime.timedelta(minutes=10)
            elif state == 'enabled_future':
                test_start_time = curr_time + datetime.timedelta(minutes=10)
                test_end_time = curr_time + datetime.timedelta(minutes=20)
            ValveTimerControllerTest.replace_timer(valve_id, 1, test_start_time, test_end_time)

    @staticmethod
    def configure_remote_job(valve_id, state):
        if state == 'empty':
            ValveRemoteJobControllerTest.remove_all_jobs(valve_id)
            return
        valve_state = past_date_time = None
        if 'off' in state:
            valve_state = 0
        elif 'on' in state:
            valve_state = 1
        if 'past' in state:
            past_date_time = datetime.datetime.now() - datetime.timedelta(minutes=30)
        if valve_state is not None:
            ValveRemoteJobControllerTest.add_job(valve_id, valve_state, past_date_time)

    def scenario_assertions(self, valve_info, timer_state, remote_state):
        scenario = "timer_state:%s and remote_state:%s" % (timer_state, remote_state)

        def expected_scenario_assertions(keys):
            msg = 'Assertions for scenario %s with keys %s' % (scenario, ",".join(keys))
            if 'off' in keys:
                self.assertEqual(valve_info['state'], 0, msg)
            elif 'on' in keys:
                self.assertEqual(valve_info['state'], 1, msg)
            else:
                pass
                self.assertTrue(False, msg)
            if 'no_id' in keys:
                self.assertIsNone(valve_info['id'], msg)
            elif 'id' in keys:
                self.assertIsNotNone(valve_info['id'], msg)
            else:
                pass
                self.assertTrue(False, msg)

        expected_scenario_keys = []
        if timer_state in ['empty', 'off', 'enabled_past', 'enabled_future']:
            if remote_state == 'empty' or 'off' in remote_state:
                expected_scenario_keys.append('off')
            else:
                expected_scenario_keys.append('on')
            if remote_state == 'empty':
                expected_scenario_keys.append('no_id')
            else:
                expected_scenario_keys.append('id')
        elif timer_state == 'enabled_current':
            if remote_state == 'off':
                expected_scenario_keys += ['off', 'id']
            else:
                expected_scenario_keys.append('on')
                if 'on' in remote_state:
                    expected_scenario_keys.append('id')
                else:
                    expected_scenario_keys.append('no_id')

        self.assertNotEqual(expected_scenario_keys, [], "Scenario not tested => %s" % scenario)
        expected_scenario_assertions(expected_scenario_keys)

    def test_1_all_scenarios(self):
        timer_states = ['empty', 'off', 'enabled_past', 'enabled_future', 'enabled_current']
        remote_states = ['empty', 'off', 'off_past', 'on', 'on_past']
        for ts in timer_states:
            ValveInfoFetchTest.configure_timer(self.valve_id, ts)
            for rs in remote_states:
                ValveInfoFetchTest.configure_remote_job(self.valve_id, rs)
                valve_info = VC.get_valve_info(self.valve_id)
                self.scenario_assertions(valve_info, ts, rs)


# Tests aggregated valve info fetch API as a client
class ValveInfoFetchAPITest(unittest.TestCase):
    def setUp(self):
        self.api_url = 'http://localhost:' + str(TEST_SERVER_PORT)
        self.product_key = '$(2#2Da$131s&*f4!x'
        self.valve_id = 3

    def test_1_fetch_open_with_id(self):
        ValveInfoFetchTest.configure_timer(self.valve_id, 'off')
        ValveInfoFetchTest.configure_remote_job(self.valve_id, 'on')
        resp = requests.get(self.api_url + '/valve', auth=HTTPBasicAuth('API_KEY', self.product_key))
        self.assertEqual(resp.status_code, 200)
        valve_info = resp.json()
        self.assertIsNotNone(valve_info['id'])
        self.assertEqual(valve_info['state'], 1)


# Tests product heart beat updates API as a client
class ProductHeartBeatAPITest(unittest.TestCase):
    def setUp(self):
        self.api_url = 'http://localhost:' + str(TEST_SERVER_PORT)
        self.product_key = '$(2#2Da$131s&*f4!x'
        self.product_id = 2

    def test_1_fetch_valve_info(self):
        DBUtilsTest.remove_heart_beats(self.product_id)
        resp = requests.get(self.api_url + '/valve', auth=HTTPBasicAuth('API_KEY', self.product_key))
        self.assertEqual(resp.status_code, 200)
        count = DBUtilsTest.get_product_heart_beat_count(self.product_id)
        self.assertEqual(count, 1)


# Tests valve info update method and associated callbacks
class ValveInfoUpdateTest(unittest.TestCase):
    def setUp(self):
        self.valve_id = 3

    @staticmethod
    def get_valve_state(valve_id):
        with DB.Connection() as cursor:
            sql = 'SELECT state FROM valve_state WHERE component_id=%s'
            count = cursor.execute(sql, (valve_id,))
            assert count <= 1
            if count == 0:
                return None
            else:
                row = cursor.fetchone()
                return row['state']

    @staticmethod
    def remove_valve_state(valve_id):
        with DB.Connection() as cursor:
            sql = 'DELETE FROM valve_state WHERE component_id=%s'
            deleted = cursor.execute(sql, (valve_id,))
            assert deleted <= 1

    def test_1_invalid_data(self):
        ValveInfoUpdateTest.remove_valve_state(self.valve_id)
        invalid_data = {'state': -1}
        self.assertRaises(ValueError, VC.update_valve_info, self.valve_id, invalid_data)
        new_state = ValveInfoUpdateTest.get_valve_state(self.valve_id)
        self.assertIsNone(new_state)

    def test_2_valve_open(self):
        ValveInfoUpdateTest.remove_valve_state(self.valve_id)
        data = {'state': 1}
        VC.update_valve_info(self.valve_id, data)
        new_state = ValveInfoUpdateTest.get_valve_state(self.valve_id)
        self.assertEqual(new_state, 1)

    def test_3_valve_close(self):
        ValveInfoUpdateTest.remove_valve_state(self.valve_id)
        data = {'state': 0}
        VC.update_valve_info(self.valve_id, data)
        new_state = ValveInfoUpdateTest.get_valve_state(self.valve_id)
        self.assertEqual(new_state, 0)

    def test_4_valve_open_state_change(self):
        self.test_3_valve_close()
        data = {'state': 1}
        VC.update_valve_info(self.valve_id, data)
        new_state = ValveInfoUpdateTest.get_valve_state(self.valve_id)
        self.assertEqual(new_state, 1)

    def test_5_valve_close_state_change(self):
        self.test_2_valve_open()
        data = {'state': 0}
        VC.update_valve_info(self.valve_id, data)
        new_state = ValveInfoUpdateTest.get_valve_state(self.valve_id)
        self.assertEqual(new_state, 0)

    def test_6_valve_close_job_complete_callback(self):
        ValveRemoteJobControllerTest.add_job(self.valve_id, 0)
        info = VC.remote_valve_controller(self.valve_id)
        self.assertIsNotNone(info['id'])
        self.assertFalse(info['completed'])
        job_id = info['id']
        data = {'state': 0, 'id': job_id}
        VC.update_valve_info(self.valve_id, data)
        new_info = VC.remote_valve_controller(self.valve_id)
        self.assertTrue(new_info['completed'])

    def test_7_valve_open_job_complete_callback(self):
        ValveRemoteJobControllerTest.add_job(self.valve_id, 1)
        info = VC.remote_valve_controller(self.valve_id)
        self.assertIsNotNone(info['id'])
        self.assertFalse(info['completed'])
        job_id = info['id']
        data = {'state': 1, 'id': job_id}
        VC.update_valve_info(self.valve_id, data)
        new_info = VC.remote_valve_controller(self.valve_id)
        self.assertTrue(new_info['completed'])

    def test_8_valve_open_job_complete_callback_second(self):
        self.test_7_valve_open_job_complete_callback()
        info = VC.remote_valve_controller(self.valve_id)
        self.assertIsNotNone(info['id'])
        self.assertTrue(info['completed'])
        job_id = info['id']
        data = {'state': 1, 'id': job_id}
        VC.update_valve_info(self.valve_id, data)
        new_info = VC.remote_valve_controller(self.valve_id)
        self.assertTrue(new_info['completed'])

    def test_9_valve_close_no_job_complete_callback(self):
        ValveRemoteJobControllerTest.add_job(self.valve_id, 0)
        info = VC.remote_valve_controller(self.valve_id)
        self.assertFalse(info['completed'])
        data = {'state': 0, 'id': None}
        VC.update_valve_info(self.valve_id, data)
        new_info = VC.remote_valve_controller(self.valve_id)
        self.assertFalse(new_info['completed'])


# Tests valve info update API as a client
class ValveInfoUpdateAPITest(unittest.TestCase):
    def setUp(self):
        self.api_url = 'http://localhost:' + str(TEST_SERVER_PORT)
        self.product_key = '$(2#2Da$131s&*f4!x'
        self.valve_id = 3

    def test_1_update_valid(self):
        ValveInfoUpdateTest.remove_valve_state(self.valve_id)
        data = {'state': 1}
        resp = requests.post(self.api_url + '/valve', json=data, auth=HTTPBasicAuth('API_KEY', self.product_key),
                             headers={'Content-Type': 'application/json'})
        self.assertEqual(resp.status_code, 200)
        new_state = ValveInfoUpdateTest.get_valve_state(self.valve_id)
        self.assertEqual(new_state, 1)

    def test_2_update_invalid(self):
        ValveInfoUpdateTest.remove_valve_state(self.valve_id)
        invalid_data = {'state': -1}
        resp = requests.post(self.api_url + '/valve', json=invalid_data,
                             auth=HTTPBasicAuth('API_KEY', self.product_key),
                             headers={'Content-Type': 'application/json'})
        self.assertEqual(resp.status_code, 400)
        new_state = ValveInfoUpdateTest.get_valve_state(self.valve_id)
        self.assertIsNone(new_state)


# Tests flow data insert method
class FlowInfoUpdateTest(unittest.TestCase):
    def setUp(self):
        self.flow_id = 4

    @staticmethod
    def get_flow_data(flow_id):
        with DB.Connection() as cursor:
            sql = 'SELECT flow_volume, flow_duration FROM flow_rate WHERE component_id=%s'
            cursor.execute(sql, (flow_id,))
            return cursor.fetchall()

    @staticmethod
    def remove_flow_data(flow_id):
        with DB.Connection() as cursor:
            sql = 'DELETE FROM flow_rate WHERE component_id=%s'
            cursor.execute(sql, (flow_id,))

    @staticmethod
    def map_data(data):
        return [{'flow_volume': d['volume'], 'flow_duration': d['duration']} for d in data]

    def test_1_insert_first(self):
        FlowInfoUpdateTest.remove_flow_data(self.flow_id)
        data = {'volume': 10, 'duration': 20}
        FC.insert_flow_info(self.flow_id, data)
        saved_data = FlowInfoUpdateTest.get_flow_data(self.flow_id)
        expected_saved_data = FlowInfoUpdateTest.map_data([data])
        self.assertEqual(saved_data, expected_saved_data)

    def test_2_insert_second(self):
        FlowInfoUpdateTest.remove_flow_data(self.flow_id)
        data = {'volume': 10, 'duration': 20}
        FC.insert_flow_info(self.flow_id, data)
        FC.insert_flow_info(self.flow_id, data)
        saved_data = FlowInfoUpdateTest.get_flow_data(self.flow_id)
        expected_saved_data = FlowInfoUpdateTest.map_data([data])
        self.assertNotEqual(saved_data, expected_saved_data)

    def test_2_insert_second_delay(self):
        FlowInfoUpdateTest.remove_flow_data(self.flow_id)
        data = {'volume': 10, 'duration': 20}
        delay = 2  # Seconds
        FC.insert_flow_info(self.flow_id, data)
        time.sleep(delay)
        FC.insert_flow_info(self.flow_id, data)
        saved_data = FlowInfoUpdateTest.get_flow_data(self.flow_id)
        self.assertEqual(len(saved_data), 2)
        latest_saved_data = saved_data[-1]
        self.assertEqual(latest_saved_data['flow_volume'], data['volume'])
        self.assertTrue(delay < latest_saved_data['flow_duration'] < delay + 1)

    def test_3_insert_many(self):
        data_size = 10
        FlowInfoUpdateTest.remove_flow_data(self.flow_id)
        data = [{'volume': random.randint(1, 10), 'duration': random.randint(1, 10)} for x in range(data_size)]
        for d in data:
            FC.insert_flow_info(self.flow_id, d)
        saved_data = FlowInfoUpdateTest.get_flow_data(self.flow_id)
        self.assertEqual(len(saved_data), data_size)
        expected_saved_data = FlowInfoUpdateTest.map_data(data)
        for i in range(data_size):
            saved_volume = saved_data[i]['flow_volume']
            expected_volume = expected_saved_data[i]['flow_volume']
            self.assertEqual(saved_volume, expected_volume)

    def test_3_invalid_insert(self):
        FlowInfoUpdateTest.remove_flow_data(self.flow_id)
        invalid_data = {'invalid': 10, 'duration': 20}
        self.assertRaises(ValueError, FC.insert_flow_info, self.flow_id, invalid_data)
        saved_data = FlowInfoUpdateTest.get_flow_data(self.flow_id)
        expected_saved_data = ()
        self.assertEqual(saved_data, expected_saved_data)


# Tests flow data insert API as a client
class FlowInfoUpdateAPITest(unittest.TestCase):
    def setUp(self):
        self.api_url = 'http://localhost:' + str(TEST_SERVER_PORT)
        self.product_key = '$(2#2Da$131s&*f4!x'
        self.flow_id = 4

    def test_1_insert_valid(self):
        FlowInfoUpdateTest.remove_flow_data(self.flow_id)
        data = {'volume': 10, 'duration': 20}
        resp = requests.post(self.api_url + '/flow', json=data, auth=HTTPBasicAuth('API_KEY', self.product_key),
                             headers={'Content-Type': 'application/json'})
        self.assertEqual(resp.status_code, 200)
        saved_data = FlowInfoUpdateTest.get_flow_data(self.flow_id)
        expected_saved_data = FlowInfoUpdateTest.map_data([data])
        self.assertEqual(saved_data, expected_saved_data)

    def test_2_insert_invalid(self):
        FlowInfoUpdateTest.remove_flow_data(self.flow_id)
        data = {}
        resp = requests.post(self.api_url + '/flow', json=data, auth=HTTPBasicAuth('API_KEY', self.product_key),
                             headers={'Content-Type': 'application/json'})
        self.assertEqual(resp.status_code, 400)
        saved_data = FlowInfoUpdateTest.get_flow_data(self.flow_id)
        expected_saved_data = ()
        self.assertEqual(saved_data, expected_saved_data)


if __name__ == "__main__":
    unittest.main()
