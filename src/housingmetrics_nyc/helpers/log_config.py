import logging

def configure_logging(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s'):
    logging.basicConfig(level=level, format=format)

# Automatically configure logging when the module is imported
configure_logging()