# All
config = {}

# Server
server_config = {
    'SERVER_PORT': 8000
}
config.update(server_config)

# DB
db_config = {
    'DB_HOST': '',
    'DB_PORT': '',
    'DB_USER': '',
    'DB_PSWD': '',
    'DB_NAME': '',
    'DB_PING_INTERVAL': 60 * 60  # Seconds
}
config.update(db_config)

# Components
comp_config = {
    'VALVE_COMP_TYPE': 'valve',
    'FLOW_COMP_TYPE': 'flow-sensor',
    'VALVE_CONTROLLER_REMOTE': 'remote',
    'VALVE_CONTROLLER_TIMER': 'timer'
}
config.update(comp_config)

# Private Overrides
try:
    from PrivateConfig import config as private_config
except ImportError:
    private_config = {}
config.update(private_config)
