import json
import logging
import psycopg2
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

def get_config():
    try:
        with open('C:\workspace\gic_data_engineering\data_engineering_test\config.json', 'r') as f:
            config = json.load(f)
            return config
    except Exception as e:
        logger.error(f"Error reading config JSON: {e}", exc_info=True)

def get_db_engine():
    config = get_config()
    engine = create_engine(f"postgresql+psycopg2://{config['db_username']}:{config['db_password']}@{config['db_host']}/{config['db_name']}")
    return engine

def push_to_db(dataframe, db_table):

    try:
        engine = get_db_engine()
        dataframe.to_sql(db_table, engine, if_exists='append', index=False)

        logger.info(f"Data inserted successfully into table: {db_table}")
    except (Exception, psycopg2.Error) as e:
        logger.error(f"Error while inserting data into {db_table}: {e}", exc_info=True)
        raise e 