
# -*- coding: UTF-8 -*-
from pyspark.sql import SparkSession
import platform
import datetime
import socket


def printevn():
    # 当前运行环境
    # 查看当前主机名
    print('当前运行服务器: ' + socket.gethostname())
    # 根据主机名称获取当前IP
    # print('IP: ' + socket.gethostbyname(socket.gethostname()))
    print('Python Version:', platform.python_version())


def getdate():
    delta = datetime.timedelta(days=1)
    today = datetime.datetime.now()
    yesterday = (today - delta).strftime('%Y-%m-%d')
    dd = yesterday[0:4] + yesterday[5:7] + yesterday[8:10]
    return yesterday, dd


if __name__ == '__main__':
    print('Start time:', datetime.datetime.now())
    # 打印环境变量
    printevn()

    yesterday, dd = getdate()
    resultHdfsPath = 'hdfs://yamicdh5/user/yamipro/warehourse/dw/event/event_utm_medium_by_day/' + dd

    sqlContext = SparkSession.builder.appName('dddddddd').config("spark.sql.warehouse.dir","/user/hive/warehouse").enableHiveSupport().getOrCreate()

    utm_medium_by_day = sqlContext.sql("""
select
    utm_medium_regrouping
    ,utm_medium
    ,platform_new
    ,sum(pv_flag) as pv
    ,count(distinct virturlSession) as sessions
    ,count(distinct user_id) as uv
    ,count(distinct reg_success_flag) as new_register_users
    ,count(distinct pay_succ_uv_flag) as customers
    ,count(distinct new_pur_user_flag) as new_customers
    ,count(distinct order_id) as orders
    ,sum(order_amount) as gmv
    ,case when count(distinct order_id) = 0 then null else sum(order_amount) / count(distinct order_id) end  as aov
    ,case when sum(order_amount) = 0 then null else (sum(order_amount) - sum(cost_amount)) / sum(order_amount)  end as margin_rate
    ,case when count(distinct virturlSession) = 0 then null else count(distinct session_bounce_flag) / count(distinct virturlSession) end as session_bouns_rate
    ,count(distinct user_id) - count(distinct user_bounce_flag) as valid_uv
    ,count(distinct addcard_uv_flag) as addcart_uv
    ,count(submit_order_uv_flag) as submit_order_uv
    ,count(distinct new_uservisit_flag) as new_visitors
    ,count(distinct new_visit_flag) as new_divices
from
    sum_event.event_allvisitor_action_detail
where
    day_id = '{yesterday}'
group by
    day_id
    ,utm_medium_regrouping
    ,utm_medium
    ,platform_new
    """.format(yesterday=yesterday))

    utm_medium_by_day.show()

    utm_medium_by_day.repartition(numPartitions=1).write.mode(saveMode="overwrite").orc(resultHdfsPath)

    sqlContext.sql(
        s"ALTER TABLE sum_event.event_utm_medium_dy_day add if not exists PARTITION (day_id='{yesterday}')  LOCATION '{warehouseLocation}'".format(
            yesterday=yesterday, warehouseLocation=resultHdfsPath))

    print('End time:', datetime.datetime.now())
    sqlContext.stop()