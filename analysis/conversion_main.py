import pandas as pd
from sqlalchemy import create_engine
from analysis.conversion import Conversion
import time

# creating connection to mysql
engine = create_engine("mysql+pymysql://root:password@localhost/test")

# Task1
sql1 = """
     SELECT (count( distinct IF(event_type = 'conversion',session_id,null))/count(distinct session_id))*100 as conv_rate from test.conversion_data
"""

# Task2
sql2 = """
select (count(distinct IF(d1.event_type = "conversion",d1.session_id,null))/count(distinct d1.session_id))*100 from
test.conversion_data d1
join
(SELECT a.timestamp,a.session_id,a.event_type,a.page_id,b.min_time FROM test.conversion_data a
join
(select min(timestamp) min_time,session_id FROM test.conversion_data group by session_id) b  on  a.session_id = b.session_id and a.timestamp = b.min_time and page_id = '4903628644844587131') d2
on d1.session_id = d2.session_id
"""

# Task3
sql3 = """
select timestamp,session_id,device_type,campaign_id,event_type,page_id from test.conversion_data order by session_id asc,page_id asc limit %s offset %s
"""


def ordered_set(sql3, engine):
    # gives the total number of rows in the table
    batch_size = 100000
    all_count = pd.read_sql(sql="select count(*) from test.conversion_data", con=engine)
    count = all_count.values.item(0)

    # fetch rows in batches of 100000 from mysql
    for offset in range(0, count, batch_size):
        sql = sql3 %(batch_size, offset)
        # print(sql)
        generator_df = pd.read_sql(sql=sql, con=engine)
        print(generator_df.to_string(index=False))


a = Conversion()
# Task1
a.conversion_rate(sql1, engine)
time.sleep(3)
# Task2
a.conversion_rate(sql2, engine)
time.sleep(3)
print("Printing ordered set:")
# Task3
ordered_set(sql3=sql3, engine=engine)
