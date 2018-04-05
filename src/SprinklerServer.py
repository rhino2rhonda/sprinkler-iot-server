import web
from SprinklerConfig import config
import ValveControl
import FlowControl
import logging

logger = logging.getLogger(__name__)

# URL handlers
urls = (
    '/valve', ValveControl.ValveRequestHandler,
    '/flow', FlowControl.FlowRequestHandler
)


# Starts the server at the given port on localhost
def start_server(port=config['SERVER_PORT']):
    app = web.application(urls, globals())
    logger.info('Starting server at: http://localhost:%d', port)
    web.httpserver.runsimple(app.wsgifunc(), ("0.0.0.0", port))
