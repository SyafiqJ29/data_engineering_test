import pandas as pd
import logging
from datetime import date

logger = logging.getLogger(__name__)


def main():
    prog_name_df = pd.read_excel(".\TO_DO_TEST.xlsx", sheet_name="PROG_NAME")
    dependency_rules_df = pd.read_excel(".\TO_DO_TEST.xlsx", sheet_name="DEPENDENCY_RULES")
    df = pd.merge(prog_name_df, dependency_rules_df, on="STEP_SEQ_ID")
    logger.info('prog_name_df and dependency_rules_df merged')

    df_sorted = df.sort_values(by='STEP_DEP_ID')
    logger.info('Dataframe sorted by STEP_DEP_ID')

    today = date.today()
    today_str = today.strftime("%Y%m%d_%H%M")

    df_sorted.to_csv(f".\sorted_names\merged_df_{today_str}.csv")
    logger.info(f'Dataframe sorted by STEP_DEP_ID saved to csv')
    print(df_sorted[['STEP_PROG_NAME']])


if __name__ == "__main__":
    logger.info('Sorting sequence process started')
    main()
    logger.info('Sorting sequence process ended')