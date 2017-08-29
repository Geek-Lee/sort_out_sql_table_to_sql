import multiprocessing
from time import ctime, sleep
import pandas as pd
import numpy as np
from pandas import DataFrame
from sqlalchemy import create_engine
import time
import sys
from help.io import to_sql

def list_to_format_columns(col, table=None):
    if table is not None:
        if isinstance(col, str):
            return '{}.{}'.format(table, col)
        elif isinstance(col, list):
            return ','.join(map(lambda x: '{}.{}'.format(table, x), col))
    else:
        if isinstance(col, str):
            return col
        elif isinstance(col, list):
            return ','.join(map(lambda x: '{}'.format(x), col))

engine_test_gt = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_read_17', 'jr_admin_read_17', '182.254.128.241', 8612, 'test_gt'),
    connect_args={"charset": "utf8"})

print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

#四个数据库的共同字段
fmreturn_sql = "SELECT * FROM fund_month_return WHERE fund_id='JR000001'"
fmrisk_sql = "SELECT * FROM fund_month_risk WHERE fund_id='JR000001'"
fsmindex_sql = "SELECT * FROM fund_subsidiary_month_index WHERE fund_id='JR000001'"
fmindicator_sql = "SELECT * FROM fund_month_indicator WHERE fund_id='JR000001'"
fmreturn_df = pd.read_sql(fmreturn_sql, engine_test_gt)
fmrisk_df = pd.read_sql(fmrisk_sql, engine_test_gt)
fsmindex_df = pd.read_sql(fsmindex_sql, engine_test_gt)
fmindicator_df = pd.read_sql(fmindicator_sql, engine_test_gt)
fmreturn_field = list_to_format_columns(list(fmreturn_df.columns.intersection(fmindicator_df.columns)), "fmr")
fmrisk_field = list_to_format_columns(list(fmrisk_df.columns.intersection(fmindicator_df.columns)), "fms")
fsmindex_field = list_to_format_columns(list(fsmindex_df.columns.intersection(fmindicator_df.columns)), "fmi")

monthly_fund_id_sql = "SELECT DISTINCT fund_id FROM fund_month_return " \
      "UNION SELECT DISTINCT fund_id FROM fund_month_risk " \
      "UNION SELECT DISTINCT fund_id FROM fund_subsidiary_month_index"
monthly_fund_id_df = pd.read_sql(monthly_fund_id_sql, engine_test_gt)

monthly_fund_id_list = monthly_fund_id_df['fund_id'].tolist()
monthly_fund_id_length = len(monthly_fund_id_list)
monthly_i = monthly_fund_id_length//4000

def worker(i):
    print("worker")
    fund_id_tuple = tuple(monthly_fund_id_list[i*1000:(i+1)*1000])
    fund_month_return_sql = "SELECT {} FROM fund_month_return fmr" \
                             " JOIN (SELECT fund_id, max(statistic_date)" \
                             " AS msd FROM fund_month_return fmr GROUP BY fund_id)" \
                             " temp ON temp.fund_id = fmr.fund_id" \
                             " AND temp.msd = fmr.statistic_date" \
                             " WHERE fmr.fund_id IN {}".format(fmreturn_field, fund_id_tuple)
    fund_month_return_df = pd.read_sql(fund_month_return_sql, engine_test_gt)
    fund_month_risk_sql = "SELECT {} FROM fund_month_risk fms" \
                           " JOIN (SELECT fund_id, max(statistic_date)" \
                           " AS msd FROM fund_month_risk fms GROUP BY fund_id)" \
                           " temp ON temp.fund_id = fms.fund_id" \
                           " AND temp.msd = fms.statistic_date" \
                           " WHERE fms.fund_id IN {}".format(fmrisk_field, fund_id_tuple)
    fund_month_risk_df = pd.read_sql(fund_month_risk_sql, engine_test_gt)
    fund_subsidiary_month_index_sql = "SELECT {} FROM fund_subsidiary_month_index fmi" \
                                       " JOIN (SELECT fund_id, max(statistic_date)" \
                                       " AS msd FROM fund_subsidiary_month_index fmi GROUP BY fund_id)" \
                                       " temp ON temp.fund_id = fmi.fund_id" \
                                       " AND temp.msd = fmi.statistic_date WHERE fmi.fund_id IN {}".format(
        fsmindex_field, fund_id_tuple)
    fund_subsidiary_month_index_df = pd.read_sql(fund_subsidiary_month_index_sql, engine_test_gt)

    df = pd.merge(fund_month_return_df, fund_month_risk_df, how='outer',
                  left_on=['fund_id', 'statistic_date', 'benchmark'],
                  right_on=['fund_id', 'statistic_date', 'benchmark'])
    df = pd.merge(df, fund_subsidiary_month_index_df, how='outer',
                  left_on=['fund_id', 'statistic_date', 'benchmark'],
                  right_on=['fund_id', 'statistic_date', 'benchmark'])
    df = df.drop(["entry_time_x", "update_time_x", "entry_time_y", "update_time_y", "entry_time", "update_time"],
                 axis=1)
    to_sql("fund_month_indicator", engine_test_gt, df, chunksize=10000)
    print("to sql!")

if __name__ == "__main__":
    for i in range(monthly_i):
        iterable_list = [4*i, 4*i+1, 4*i+2, 4*i+3]
        pool = multiprocessing.Pool(processes=10)
        pool.map(worker, iterable_list)
        pool.close()
        pool.join()
        print("all over %s" % ctime())
    print("over!")