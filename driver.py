import sys

import get_env_variables as gev
from create_spark import get_spark_objet
from validate import get_current_date
import logging
import logging.config

logging.config.fileConfig('Properties/configuration/logging.conf')


def main():
    try:
        logging.info(" i am beginning with the main app .......")

        logging.info('creation of th spark object .....')
        spark = get_spark_objet(gev.envn, gev.appName)

        logging.info(f" this is the spark object created : {spark}")

        logging.info('validation spark object ....')
        get_current_date(spark)

    except Exception as e:
        logging.info(f"we have an error with a exception : {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
    logging.info('application done, Good Job .....')
