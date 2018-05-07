# -*- coding:utf-8 -*-
"""
Report Data Access Object
"""
from app.connector.MySQLConnect import connect
import pandas as pd


def load_sensor_group_data(date):
    """
    acquire normal sensor data from database
    :return: data frame object
    """
    db = connect()
    cursor = db.cursor()
    cursor.execute("select tb2.eui,tb2.temperature 温度,tb2.humidity 湿度, tb2.batt 电量,tb2.current 电流,tb2.voltage 电压,tb2.power 功率,tb2.ts 获取时间,tb1.name 传感器名称,tb3.name 所属设备,tb4.name 所属酒店\
                                from tb_sensor_data tb2 LEFT JOIN tb_sensor tb1 on tb1.code = tb2.eui LEFT JOIN tb_equipment tb3 on tb3.equipment_id = tb1.equipment_id \
                                LEFT JOIN tb_customer tb4 on tb4.customer_id = tb1.customer_id where (tb2.eui='9896830000000008' or tb2.eui='9896830000000002' or tb2.eui = '9896830000000003' \
                                 or tb2.eui='9896830000000004' or tb2.eui = '9896830000000006' or tb2.eui = '3786E6ED0034004B' or \
                                 tb2.eui = '3768B26900230053' or tb2.eui = '4768B269002B0059' or tb2.eui = '4768B269001F003F' or \
                                 tb2.eui='4778B269003B002F' or tb2.eui='3430363057376506' or tb2.eui='3430363067378B07' or tb2.eui = '3430363064378607' \
                                 or tb2.eui='343036305D375E05' or tb2.eui = '3430363064378007') and tb2.ts > '" + date +"' ORDER BY tb2.ts ASC")
    results = []
    for row in cursor.fetchall():
        results.append(
            {'eui': row[0], '温度': row[1], '湿度': row[2], '电量': row[3], '电流': row[4], '电压': row[5], '功率': row[6],
             '获取时间': row[7], '传感器名称': row[8], '所属设备': row[9], '所属酒店': row[10]})
    df = pd.DataFrame(data=results)
    return df
