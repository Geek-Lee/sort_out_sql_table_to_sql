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
fwreturn_sql = "SELECT * FROM fund_weekly_return WHERE fund_id='JR000001'"
fwrisk_sql = "SELECT * FROM fund_weekly_risk WHERE fund_id='JR000001'"
fswindex_sql = "SELECT * FROM fund_subsidiary_weekly_index WHERE fund_id='JR000001'"
fwindicator_sql = "SELECT * FROM fund_weekly_indicator WHERE fund_id='JR000001'"
fwreturn_df = pd.read_sql(fwreturn_sql, engine_test_gt)
fwrisk_df = pd.read_sql(fwrisk_sql, engine_test_gt)
fswindex_df = pd.read_sql(fswindex_sql, engine_test_gt)
fwindicator_df = pd.read_sql(fwindicator_sql, engine_test_gt)
fwreturn_field = list_to_format_columns(list(fwreturn_df.columns.intersection(fwindicator_df.columns)), "fwr")
fwrisk_field = list_to_format_columns(list(fwrisk_df.columns.intersection(fwindicator_df.columns)), "fws")
fswindex_field = list_to_format_columns(list(fswindex_df.columns.intersection(fwindicator_df.columns)), "fwi")

weekly_fund_id_sql = "SELECT DISTINCT fund_id FROM fund_weekly_return " \
      "UNION SELECT DISTINCT fund_id FROM fund_weekly_risk " \
      "UNION SELECT DISTINCT fund_id FROM fund_subsidiary_weekly_index"
weekly_fund_id_df = pd.read_sql(weekly_fund_id_sql, engine_test_gt)

weekly_fund_id_list = weekly_fund_id_df['fund_id'].tolist()
weekly_fund_id_length = len(weekly_fund_id_list)
weekly_i = weekly_fund_id_length//10000

def worker(k, l, fwreturn_field, fwrisk_field, fswindex_field):
    print("worker")
    fund_id_tuple = tuple(weekly_fund_id_list[k:l])
    fund_weekly_return_sql = "SELECT {} FROM fund_weekly_return fwr" \
                             " JOIN (SELECT fund_id, max(statistic_date)" \
                             " AS msd FROM fund_weekly_return fwr GROUP BY fund_id)" \
                             " temp ON temp.fund_id = fwr.fund_id" \
                             " AND temp.msd = fwr.statistic_date" \
                             " WHERE fwr.fund_id IN {}".format(fwreturn_field, fund_id_tuple)
    fund_weekly_return_df = pd.read_sql(fund_weekly_return_sql, engine_test_gt)
    fund_weekly_risk_sql = "SELECT {} FROM fund_weekly_risk fws" \
                           " JOIN (SELECT fund_id, max(statistic_date)" \
                           " AS msd FROM fund_weekly_risk fws GROUP BY fund_id)" \
                           " temp ON temp.fund_id = fws.fund_id" \
                           " AND temp.msd = fws.statistic_date" \
                           " WHERE fws.fund_id IN {}".format(fwrisk_field, fund_id_tuple)
    fund_weekly_risk_df = pd.read_sql(fund_weekly_risk_sql, engine_test_gt)
    fund_subsidiary_weekly_index_sql = "SELECT {} FROM fund_subsidiary_weekly_index fwi" \
                                       " JOIN (SELECT fund_id, max(statistic_date)" \
                                       " AS msd FROM fund_subsidiary_weekly_index fwi GROUP BY fund_id)" \
                                       " temp ON temp.fund_id = fwi.fund_id" \
                                       " AND temp.msd = fwi.statistic_date WHERE fwi.fund_id IN {}".format(
        fswindex_field, fund_id_tuple)
    fund_subsidiary_weekly_index_df = pd.read_sql(fund_subsidiary_weekly_index_sql, engine_test_gt)

    df = pd.merge(fund_weekly_return_df, fund_weekly_risk_df, how='outer',
                  left_on=['fund_id', 'statistic_date', 'benchmark'],
                  right_on=['fund_id', 'statistic_date', 'benchmark'])
    df = pd.merge(df, fund_subsidiary_weekly_index_df, how='outer',
                  left_on=['fund_id', 'statistic_date', 'benchmark'],
                  right_on=['fund_id', 'statistic_date', 'benchmark'])
    df = df.drop(["entry_time_x", "update_time_x", "entry_time_y", "update_time_y", "entry_time", "update_time"],
                 axis=1)
    to_sql("fund_weekly_indicator", engine_test_gt, df, chunksize=10000)
    print("to sql!")

if __name__ == "__main__":
    for i in range(weekly_i):
        p = multiprocessing.Process(target=worker, args=(10*i*1000, (10*i+1)*1000, fwreturn_field, fwrisk_field, fswindex_field,))
        p2 = multiprocessing.Process(target=worker, args=((10*i+1)*1000, (10*i+2)*1000, fwreturn_field, fwrisk_field, fswindex_field,))
        p3 = multiprocessing.Process(target=worker, args=((10*i+2)*1000, (10*i+3)*1000, fwreturn_field, fwrisk_field, fswindex_field,))
        p4 = multiprocessing.Process(target=worker, args=((10*i+3)*1000, (10*i+4)*1000, fwreturn_field, fwrisk_field, fswindex_field,))
        p5 = multiprocessing.Process(target=worker, args=((10*i+4)*1000, (10*i+5)*1000, fwreturn_field, fwrisk_field, fswindex_field,))
        p6 = multiprocessing.Process(target=worker, args=((10*i+5)*1000, (10*i+6)*1000, fwreturn_field, fwrisk_field, fswindex_field,))
        p7 = multiprocessing.Process(target=worker, args=((10*i+6)*1000, (10*i+7)*1000, fwreturn_field, fwrisk_field, fswindex_field,))
        p8 = multiprocessing.Process(target=worker, args=((10*i+7)*1000, (10*i+8)*1000, fwreturn_field, fwrisk_field, fswindex_field,))
        p9 = multiprocessing.Process(target=worker, args=((10*i+8)*1000, (10*i+9)*1000, fwreturn_field, fwrisk_field, fswindex_field,))
        p10 = multiprocessing.Process(target=worker, args=((10*i+9)*1000, (10*i+10)*1000, fwreturn_field, fwrisk_field, fswindex_field,))
        # p = multiprocessing.Process(target=worker, args=(i,))
        # p2 = multiprocessing.Process(target=worker2, args=(i,))
        # p3 = multiprocessing.Process(target=worker3, args=(i,))
        # p4 = multiprocessing.Process(target=worker4, args=(i,))
        # p5 = multiprocessing.Process(target=worker5, args=(i,))
        # p6 = multiprocessing.Process(target=worker6, args=(i,))
        # p7 = multiprocessing.Process(target=worker7, args=(i,))
        # p8 = multiprocessing.Process(target=worker8, args=(i,))
        # p9 = multiprocessing.Process(target=worker9, args=(i,))
        # p10 = multiprocessing.Process(target=worker10, args=(i,))
        p.daemon = True
        p2.daemon = True
        p3.daemon = True
        p4.daemon = True
        p5.daemon = True
        p6.daemon = True
        p7.daemon = True
        p8.daemon = True
        p9.daemon = True
        p10.daemon = True
        p.start()
        p2.start()
        p3.start()
        p4.start()
        p5.start()
        p6.start()
        p7.start()
        p8.start()
        p9.start()
        p10.start()
        p.join()
        p2.join()
        p3.join()
        p4.join()
        p5.join()
        p6.join()
        p7.join()
        p8.join()
        p9.join()
        p10.join()
        print("all over %s" % ctime())
    print("over!")


