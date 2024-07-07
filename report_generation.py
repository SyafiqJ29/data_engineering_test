import pandas as pd
from sqlalchemy import create_engine
from common import *

config = get_config()
engine = get_db_engine()
db_table = 'eom_report'

funddate = '2023-07-31' # to be configured to take in input

def combine_non_nan(row):
  combined_value = row['PRICE_x'] if pd.notna(row['PRICE_x']) else row['PRICE_y']
  return combined_value

def main():

  fund_sql = f"""
  SELECT "SECURITY NAME" , "FINANCIAL TYPE", "ISIN", "SYMBOL", "PRICE", "FUND DATE","FUND NAME"
  FROM funds 
  where "FUND DATE" = '{funddate}'"""
  bond_sql = f"""
  SELECT br."ISIN", bp."PRICE"
  FROM bond_reference br
  LEFT JOIN bond_prices bp
  ON br."ISIN"= bp."ISIN"
  WHERE "DATETIME" = '{funddate}'
  """
  equity_sql = f"""
  SELECT er."SYMBOL", ep."PRICE",ep."DATETIME"
  FROM equity_reference er
  LEFT JOIN equity_prices ep
  ON er."SYMBOL"= ep."SYMBOL"
  WHERE "DATETIME" = '{funddate}'
  """

  fund_df = pd.read_sql(fund_sql, engine)
  fund_df = fund_df.rename(columns={'PRICE': 'FUND_PRICE'})
  bond_df = pd.read_sql(bond_sql, engine)
  equity_df = pd.read_sql(equity_sql, engine)


  merged_df = fund_df.merge(bond_df, on='ISIN', how='left')
  eom_report_df = merged_df[["SECURITY NAME", "FINANCIAL TYPE", "ISIN", "SYMBOL", "FUND_PRICE", "FUND DATE", "PRICE"]].merge(equity_df[["SYMBOL", "PRICE"]], on='SYMBOL', how='left')

  eom_report_df['REF_PRICE'] = eom_report_df.apply(combine_non_nan, axis=1)
  eom_report_df = eom_report_df.drop('PRICE_x', axis=1) 
  eom_report_df = eom_report_df.drop('PRICE_y', axis=1) 
  eom_report_df = eom_report_df.rename(columns={'FUND DATE': 'DATETIME'})
  eom_report_df['PRICE DIFF'] = eom_report_df['REF_PRICE'] - eom_report_df['FUND_PRICE']


  # push_to_db(eom_report_df, db_table)

  # this can be saved into csv and save it in S3 as well

if __name__ == "__main__":
    logger.info("EOM Report generation Started")
    main()
    # logger.info(f"{report_month} EOM Report generation ended")
