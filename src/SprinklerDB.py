import pymysql, threading, time, logging, re, base64, web
from SprinklerConfig import config
import SprinklerUtils as Utils

logger = logging.getLogger(__name__)


# A singleton class for using and maintaining a DB connection
class Connection(Utils.Singleton):
    def init(self):
        self.logger = logging.getLogger(__name__ + ".Connection")
        self.lock = threading.RLock()
        self.lock_count = 0
        self.connection = self.create_connection()
        self.cursor = None
        # Start a keep alive daemon thread
        self.active = True
        self.keep_alive_thread = threading.Thread(target=self.keep_alive, name="keep-alive-thread")
        self.keep_alive_thread.setDaemon(True)
        self.keep_alive_thread.start()
        self.logger.info("Connection manager is up and running")

    # Initializes a new DB connection
    def create_connection(self):
        self.logger.info("Creating a new DB connection")
        conn = pymysql.connect(
            host=config['DB_HOST'],
            port=config['DB_PORT'],
            user=config['DB_USER'],
            password=config['DB_PSWD'],
            db=config['DB_NAME'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        self.logger.info("Created a new DB connection")
        return conn

    # Closes DB connection
    def close_connection(self):
        self.logger.info("Closing DB connection")
        self.active = False
        self.connection.close()

    # The context of the with block is set as a cursor to the existing connection
    # Only one thread can use the with block at one time
    def __enter__(self):
        self.logger.debug("Entering with block. Acquiring thread level lock on DB connection. Lock count: %d",
                          self.lock_count)
        self.lock.acquire()
        parent_lock = self.lock_count == 0
        self.logger.debug('Is parent lock: %s', parent_lock)
        if parent_lock:
            self.logger.debug('Initializing transaction and cursor')
            self.connection.begin()
            self.cursor = self.connection.cursor()
        self.lock_count += 1
        return self.cursor

    # Cleans up the cursor and releases the lock after the with block terminates
    def __exit__(self, ex_type, ex_val, ex_trace):
        logger.debug('Exiting with block')
        parent_lock = self.lock_count == 1
        self.logger.debug('Is parent lock: %s', parent_lock)
        if parent_lock:
            if ex_val is None:
                try:
                    self.connection.commit()
                    self.logger.debug("Transaction has been committed")
                except Exception as ex:
                    self.logger.exception("Failed to commit transaction")
            else:
                try:
                    self.connection.rollback()
                    self.logger.warn("Transaction has been rolled back")
                except Exception as ex:
                    self.logger.exception("Failed to rollback transaction")
            try:
                self.cursor.close()
                self.cursor = None
                self.logger.debug('Cursor has been closed')
            except Exception as ex:
                self.logger.exception("Failed to close cursor to DB connection")
        self.lock_count -= 1
        self.lock.release()

    # Checks if the connection is alive and revives it if it is not
    def revive_connection(self):
        self.logger.debug("Attempting to revive DB connection (if it is dead)")
        try:
            with self.lock:
                self.connection.ping(True)
            return True
        except Exception as ex:
            self.logger.exception("Connection lost to DB")
            return False

    # To be executed as a thread to ensure that the connection to the database is kept alive
    def keep_alive(self):
        self.logger.debug("Keep alive thread is up and running")
        while self.active:
            time.sleep(config['DB_PING_INTERVAL'])
            revived = self.revive_connection()
            self.logger.debug("Keep alive process completed with status %s. Will retry in %d seconds", revived,
                              config['DB_PING_INTERVAL'])
        self.logger.debug("Keep alive thread will now terminate")


# Fetches the Product ID for a Product Key
def get_product_id(product_key):
    logger.debug('Fetching Product ID for Product Key %s', product_key)
    with Connection() as cursor:
        sql = 'SELECT id FROM product WHERE product_key=%s'
        count = cursor.execute(sql, (product_key,))
        if count > 0:
            row = cursor.fetchone()
            product_id = row['id']
        else:
            product_id = None
    logger.debug('Fetched Product ID %s for Product Key %s', product_id, product_key)
    return product_id


# Basic auth - If Product Key is specified in the request header, the corresponding
# Product ID is fetched and returned, otherwise None is returned
def authorize_product():
    logger.debug('Beginning basic auth using request header')
    auth = web.ctx.env.get('HTTP_AUTHORIZATION')
    if auth is None:
        return None
    auth = re.sub('^Basic ', '', auth)
    product_key = base64.decodestring(auth).split(':')[1]
    product_id = get_product_id(product_key)
    logger.debug('Completed basic auth using request header. Product ID: %s', product_id)
    return product_id


# Fetches the Component ID for a Product ID and Component Type
def get_component_id(product_id, component_type):
    logger.debug('Fetching Component ID for Product ID %d and Component Type %s', product_id, component_type)
    with Connection() as cursor:
        sql = 'SELECT component.id FROM component JOIN component_type ' + \
              'ON component.component_type_id=component_type.id ' + \
              'WHERE component.product_id=%s AND component_type.component_name=%s'
        count = cursor.execute(sql, (product_id, component_type))
        if count > 0:
            row = cursor.fetchone()
            component_id = row['id']
        else:
            component_id = None
    logger.debug('Fetched Component ID %s for Product ID %d and Component Type %s', component_id, product_id,
                 component_type)
    return component_id


# Saves a heart beat corresponding to a Product ID
def send_heart_beat(product_id):
    logger.debug('Saving heart beat for Product ID %d', product_id)
    with Connection() as cursor:
        sql = 'INSERT INTO product_heart_beat (product_id) VALUES (%s)'
        inserted = cursor.execute(sql, (product_id,))
    logger.info('Heart beat saved for Product ID %d. Rows inserted: %s', product_id, inserted)
