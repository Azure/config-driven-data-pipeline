import cddp
import cddp.dbxapi as dbxapi

# disable informational messages from prophet
import logging
logging.getLogger('py4j').setLevel(logging.ERROR)

if __name__ == "__main__":
    cddp.entrypoint()