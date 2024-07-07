import datetime

import pandas as pd
import os

from sqlalchemy import create_engine
import json
import logging
import shutil
from common import *

logger = logging.getLogger(__name__)
db_table = "funds"
consumed_folder = ".\\consumed_csv"


def get_fund_mapping():
    try:
        with open('C:\\workspace\\gic_data_engineering\\data_engineering_test\\fundnames_mapping.json', 'r') as f:
            fund_mapping = json.load(f)
            return fund_mapping
    except Exception as e:
        logger.error(f"Error reading funds mapping JSON: {e}", exc_info=True)


def get_fundname_fund_date(filename, fund_mapping):
    name_chunk = filename.split('.')[0]
    fundname = fund_mapping[name_chunk][0]

    date_string = filename.split('.')[1].split(" ")[0]
    date_format = fund_mapping[name_chunk][1]
    date_object = datetime.datetime.strptime(date_string, date_format)
    fund_date = date_object.strftime("%Y-%m-%d")
    return fundname, fund_date



def main():
    try:
        logger.info(f"CSV ETL process started")
        config = get_config()
        folder_path = config['csv_folder_path']
        if not os.path.isdir(config['csv_folder_path']):
            raise ValueError(f"Not found: {folder_path}")

        for filename in os.listdir(folder_path):
            print("filename",filename)
            if filename.endswith(".csv"):
                filepath = os.path.join(folder_path, filename)
                logger.info(f"Current file path: {filepath}")

                fund_mapping = get_fund_mapping()
                fundname, fund_date = get_fundname_fund_date(filename, fund_mapping)


                dataframe = pd.read_csv(filepath)
                dataframe['FUND NAME'] = fundname
                dataframe['FUND DATE'] = fund_date

                push_to_db(dataframe, db_table)
                
                os.makedirs(consumed_folder, exist_ok=True)
                destination_filepath = consumed_folder + f"\\{filename}"
                try:
                    shutil.move(filepath, destination_filepath)
                    logger.info(f"Successfully moved '{filepath}' to '{destination_filepath}'.")
                except Exception as e:
                    logger.error(f"Error moving file: {e}", exc_info=True)
        logger.info(f"CSV ETL process completed")

    except Exception as e:
        logger.error(f'Error:{e}', exc_info=True)


if __name__ == "__main__":

    main()
