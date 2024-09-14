import logging

def setup_logging():
    logging.basicConfig(
        filename='app.log', # log file name, can replace with a filepath
        level=logging.DEBUG,  # Logs messages from DEBUG level and above
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

