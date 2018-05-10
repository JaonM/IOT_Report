# -*- coding:utf-8 -*-
"""
data analyze service
"""
from app.dao.ReportDao import load_sensor_group_data
import math
import pandas as pd
from app.configs.hotel_sensor_config import sensor_config
import datetime


def threshold_load():
    threshold = {'9896830000000008': {'mintemperature': -22, 'maxtemperature': -10},
                 '9896830000000002': {'mintemperature': -22, 'maxtemperature': -10},
                 '9896830000000003': {'mintemperature': 0, 'maxtemperature': 10},
                 '9896830000000004': {'mintemperature': 2, 'maxtemperature': 10},
                 '9896830000000006': {'mintemperature': -22, 'maxtemperature': -10},
                 '3786E6ED0034004B': {'mintemperature': -22, 'maxtemperature': -10},
                 '3768B26900230053': {'mintemperature': -22, 'maxtemperature': -10},
                 '4768B269002B0059': {'mintemperature': 0, 'maxtemperature': 10},
                 '4768B269001F003F': {'mintemperature': 2, 'maxtemperature': 10},
                 '4778B269003B002F': {'mintemperature': -22, 'maxtemperature': 10}}
    return threshold


def analyze_data(date=datetime.datetime.now().date().strftime('%Y-%m-%d'), interval_period=60):
    """
    err_code:   0 normal    1 error -1 missing

    :param interval_period:
    :param date:
    :return:
    """
    # if file_name is None:
    #     print('file name can not be none...quiting...')
    # df = pd.read_excel(input_path + file_name)
    # threshold = load_threshold(threshold_file)
    df = load_sensor_group_data(date)
    threshold = threshold_load()
    result = dict()
    result['date'] = date
    result['results'] = list()
    df.index = pd.to_datetime(df['获取时间'])
    dimensions = ['温度', '湿度', '电量', '电流', '电压', '功率']
    dimension_threshold_mapping = {'温度': ('mintemperature', 'maxtemperature'), '湿度': ('minhumidity', 'maxhumidity'),
                                   '电量': ('minbatt', 'maxbatt'), '电流': ('mincurrent', 'maxcurrent'),
                                   '电压': ('minvoltage', 'maxvoltage'), '功率': ('minpower', 'maxpower')}

    threshold_dimension = ['温度']  # 需要阈值分析的维度

    # for sensor_name in df['eui'].unique():
    for key in sensor_config.keys():
        for sensor_name in sensor_config[key]:
            sensor = df[df['eui'] == sensor_name['eui']]

            try:
                sensor = sensor[date]
                # sensor = sensor[sensor['获取时间'] > date ]
                # sensor = sensor[sensor['获取时间'] < '2018-02-09' + ' 11:30:00']
                messages = dict()
                err_code = 0
                # handle temperature
                for dimension in dimensions:
                    # print(sensor)
                    data = sensor[dimension]
                    # print(data.values)
                    msg = list()
                    if len(data.values) > 0:
                        # dimension = '温度'
                        basic = data.describe()
                        # print(basic)
                        try:
                            msg.append({'status': 'normal', 'msg': dimension + '平均值为: ' + str(basic['mean']) + ','})
                            msg.append({'status': 'normal', 'msg': dimension + '最小值为: ' + str(basic['min']) + ','})
                            msg.append({'status': 'normal', 'msg': dimension + '最大值为: ' + str(basic['max']) + ','})
                        except KeyError:
                            pass
                        if dimension not in threshold_dimension:
                            messages[dimension] = msg
                            continue
                        try:
                            if basic['min'] < threshold[sensor_name['eui']][dimension_threshold_mapping[dimension][0]]:
                                err_code = 1
                                min_time = sensor[sensor[dimension] == basic['min']]['获取时间']
                                # min_time = min_time.time().strftime('%H:%m:%s')
                                # print(min_time)
                                min_time = min_time.map(lambda x: str(x).split()[1])
                                msg.append({'status': 'red', 'msg': dimension + '最小值低于阈值下限(' + str(
                                    threshold[sensor_name['eui']][
                                        dimension_threshold_mapping[dimension][0]]) + ')，时间为:' + str(
                                    min_time.values) + '\n'})
                            if basic['max'] > threshold[sensor_name['eui']][dimension_threshold_mapping[dimension][1]]:
                                err_code = 1
                                max_time = sensor[sensor[dimension] == basic['max']]['获取时间']
                                # print(max_time.values)
                                max_time = max_time.map(lambda x: str(x).split()[1])
                                # max_time = max_time.time().strftime('%H:%m:%s')
                                # print(max_time.values)
                                msg.append({'status': 'red', 'msg': dimension + '最大值高于阈值上限(' + str(
                                    threshold[sensor_name['eui']][
                                        dimension_threshold_mapping[dimension][1]]) + ')，时间为: ' + str(
                                    max_time.values) + '\n'})
                        except KeyError:
                            pass
                    messages[dimension] = msg

                times = sensor['获取时间']
                times = pd.to_datetime(times)
                msgs = []
                if len(times) > 1:
                    """
                    缺失丢包分析
                    """
                    # 获取时间频率判断 平均获取频率 增加首尾时间

                    _discrete_loss_times = 0  # 离散丢包次数
                    _total_loss_time = 0  # 缺失总时间

                    second_delta = 0
                    for i in range(1, len(times)):
                        delta = times[i] - times[i - 1]
                        second_delta += delta.total_seconds()
                    avg_minutes = (second_delta / (len(times) - 1)) / 60
                    msgs.append(
                        {'status': 'red', 'msg': '\n该设备平均获取时间频率为: ' + str(round(avg_minutes, 3)) + '分钟\n'})

                    # 计算时间方差
                    square_error = 0
                    for i in range(1, len(times)):
                        delta = times[i] - times[i - 1]
                        square_error = square_error + (delta.total_seconds() / 60 - avg_minutes) ** 2
                    square_error = square_error / (len(times) - 1)
                    msgs.append({'status': 'red', 'msg': '\n该设备获取时间方差为: ' + str(square_error) + '\n'})

                    # 计算丢包频率
                    lost_count = 0
                    # 开始丢包时间
                    _start_loss = 0
                    _start_loss_time = 0
                    _end_loss_time = 0
                    _continuous_lost_flag = False  # 是否连续丢包标记符

                    #
                    # 每个间隔查看丢包丢包情况
                    #
                    _packet_received_count = 1
                    start_time_flag = times[0]
                    _packet_lost_count = 0  # 丢包数量计数
                    for i in range(1, len(times)):
                        if (times[i] - start_time_flag).total_seconds() < interval_period * 60:
                            _packet_received_count += 1
                            delta = times[i] - times[i - 1]
                            if delta.total_seconds() > sensor_name['frequency']:
                                _packet_lost_count += math.floor(
                                    delta.total_seconds() / sensor_name['require_frequency'])

                            # 统计最后一个
                            if i == len(times) - 1:
                                _interval_time = (times[i] - start_time_flag).total_seconds()
                                diff = math.floor(
                                    _interval_time / sensor_name['require_frequency']) - _packet_received_count
                                _packet_lost_rate = round(diff / (_interval_time / sensor_name['require_frequency']), 2)
                                if _packet_lost_rate > 0.3:
                                    msgs.append({'status': 'red',
                                                 'msg': '在' + start_time_flag.strftime('%H:%M:%S') + '至' + times[
                                                     i].strftime(
                                                     '%H:%M:%S') + '时间中，应接收次数为' + str(
                                                     math.floor(_interval_time / sensor_name['require_frequency'])
                                                 ) + '，实际接收次数为' + str(_packet_received_count) + '，丢包率为' + str(
                                                     _packet_lost_rate) + '\n'})
                                else:
                                    msgs.append({'status': 'blue',
                                                 'msg': '在' + start_time_flag.strftime('%H:%M:%S') + '至' + times[
                                                     i].strftime(
                                                     '%H:%M:%S') + '时间中，应接收次数为' + str(
                                                     math.floor(_interval_time / sensor_name['require_frequency'])
                                                 ) + '，实际接收次数为' + str(_packet_received_count) + '，丢包率为' + str(
                                                     _packet_lost_rate) + '\n'})
                                _packet_lost_count = 0
                                start_time_flag = times[i]
                                _packet_received_count = 0
                        else:
                            # 超过周期间隔,统计当前丢失情况
                            _interval_time = (times[i] - start_time_flag).total_seconds()
                            diff = math.floor(
                                _interval_time / sensor_name['require_frequency'] - _packet_received_count)
                            _packet_lost_rate = round(diff / (_interval_time / sensor_name['require_frequency']), 2)
                            if _packet_lost_rate > 0.3:
                                msgs.append({'status': 'red',
                                             'msg': '在' + start_time_flag.strftime('%H:%M:%S') + '至' + times[
                                                 i].strftime(
                                                 '%H:%M:%S') + '时间中，应接收次数为' + str(
                                                 math.floor(_interval_time / sensor_name['require_frequency'])
                                             ) + '，实际接收次数为' + str(_packet_received_count) + '，丢包率为' + str(
                                                 _packet_lost_rate) + '\n'})
                            else:
                                msgs.append({'status': 'blue',
                                             'msg': '在' + start_time_flag.strftime('%H:%M:%S') + '至' + times[
                                                 i].strftime(
                                                 '%H:%M:%S') + '时间中，应接收次数为' + str(
                                                 math.floor(_interval_time / sensor_name['require_frequency'])
                                             ) + '，实际接收次数为' + str(_packet_received_count) + '，丢包率为' + str(
                                                 _packet_lost_rate) + '\n'})
                            _packet_lost_count = 0
                            start_time_flag = times[i]
                            _packet_received_count = 0

                    for i in range(0, len(times)):
                        _is_loss_flag = 0  # 是否重复结算连续丢包
                        if i == 0:
                            delta = times[i] - datetime.datetime.strptime(date, '%Y-%m-%d')
                            if delta.total_seconds() > 3600:  # 大于1小时算缺失
                                lost_count += math.floor(delta.total_seconds() / sensor_name['require_frequency'])
                                '''数据缺失情况'''
                                msgs.append({'status': 'red',
                                             'msg': '缺失数据时间大于1小时' + '分钟时间为,' + date + ' 0:00:00' + ',' + times[
                                                 i].strftime('%Y-%m-%d %H:%M:%S') + ',' + str(
                                                 math.floor(delta.total_seconds() / 60)) + ' 分钟,' + str(
                                                 math.ceil(delta.total_seconds() / sensor_name[
                                                     'require_frequency'])) + '次\n'})
                                _total_loss_time += math.floor(delta.total_seconds() / 60)
                                continue
                        else:
                            delta = times[i] - times[i - 1]

                        if delta.total_seconds() > sensor_name['frequency']:

                            if delta.total_seconds() > 3600:
                                '''
                                若缺失数据时间段包括丢包时间则先结算丢包时间
                                '''
                                if _start_loss == 1 and _start_loss_time != 0:
                                    _start_loss = 0
                                    time_diff = times[i - 1] - _start_loss_time
                                    # msgs.append(
                                    #     {'status': 'blue',
                                    #      'msg': '结束丢包时间为, ' + times[i - 1].strftime('%Y-%m-%d %H:%M:%S') + ',' + str(
                                    #          math.floor(
                                    #              time_diff.total_seconds() / sensor_name[
                                    #                  'require_frequency']) - 1) + '次\n'})
                                    _discrete_loss_times += math.floor(
                                        time_diff.total_seconds() / sensor_name['require_frequency']) - 1
                                lost_count += math.floor(delta.total_seconds() / sensor_name['require_frequency'])

                                '''数据缺失情况'''
                                msgs.append({'status': 'red',
                                             'msg': '缺失数据时间大于1小时' + '分钟时间为,' + times[
                                                 i - 1].strftime(
                                                 '%Y-%m-%d %H:%M:%S') + ',' + times[i].strftime(
                                                 '%Y-%m-%d %H:%M:%S') + ',' + str(
                                                 math.floor(delta.total_seconds() / 60)) + ' 分钟,' + str(math.ceil(
                                                 delta.total_seconds() / sensor_name['require_frequency'])) + '次\n'})
                                _total_loss_time += math.floor(delta.total_seconds() / 60)
                                continue

                            if _start_loss == 0:
                                _start_loss = 1
                                _start_loss_time = times[i - 1]
                                _count = i
                                # msgs.append(
                                #     {'status': 'blue',
                                #      'msg': '开始丢包时间为, ' + times[i - 1].strftime('%Y-%m-%d %H:%M:%S') + ','})
                        elif delta.total_seconds() <= sensor_name['frequency']:
                            if _start_loss == 1:
                                _start_loss = 0
                                _end_loss_time = times[i - 1]
                                _is_loss_flag = 1
                                _count = i - _count
                                time_diff = _end_loss_time - _start_loss_time
                                lost_count += math.floor(
                                    time_diff.total_seconds() / sensor_name['require_frequency'] - 1)
                                _discrete_loss_times += math.floor(
                                    time_diff.total_seconds() / sensor_name['require_frequency'] - 1)
                                # msgs.append(
                                #     {'status': 'blue',
                                #      'msg': '结束丢包时间为, ' + times[i - 1].strftime('%Y-%m-%d %H:%M:%S') + ',' + str(
                                #          math.floor(
                                #              time_diff.total_seconds() / sensor_name[
                                #                  'require_frequency']) - 1) + '次\n'})
                        if _end_loss_time is not 0 and _start_loss_time is not 0 and _is_loss_flag == 1:
                            time_diff = _end_loss_time - _start_loss_time
                            if time_diff.total_seconds() > 6 * sensor_name['frequency']:
                                _continuous_lost_flag = True
                                # msgs.append(
                                #     {'status': 'normal',
                                #      'msg': '持续丢包大于' + str(math.floor(
                                #          6 * sensor_name['frequency'] / 60)) + '分钟 丢包周期为,' + _start_loss_time.strftime(
                                #          '%Y-%m-%d %H:%M:%S') + ' , ' + _end_loss_time.strftime(
                                #          '%Y-%m-%d %H:%M:%S') + ', ' + str(math.floor(
                                #          time_diff.total_seconds() / sensor_name['frequency'])) + '次,' + str(
                                #          _count - 1) + '次' + '\n'
                                #      })

                    if not _continuous_lost_flag and lost_count != 0:
                        '''
                        没有连续丢包的情况
                        '''

                    '''与获取时间点相比计算缺失情况'''
                    now = datetime.datetime.now()
                    if now < datetime.datetime.strptime(date + ' 23:59:59', '%Y-%m-%d %H:%M:%S'):
                        delta = now - times[-1]
                        if delta.total_seconds() > 6 * sensor_name['frequency']:
                            lost_count += math.floor(delta.total_seconds() / sensor_name['require_frequency'])
                            msgs.append({'status': 'red',
                                         'msg': '缺失数据时间大于' + str(
                                             math.floor(6 * sensor_name['frequency'] / 60)) + '分钟时间为,' + times[
                                                    -1].strftime('%Y-%m-%d %H:%M:%S') + ',' + now.strftime(
                                             '%Y-%m-%d %H:%M:%S') + ',' + str(
                                             math.floor(delta.total_seconds() / 60)) + ' 分钟,' + str(math.ceil(
                                             delta.total_seconds() / sensor_name['require_frequency'])) + '次\n'})
                            _total_loss_time += math.floor(delta.total_seconds() / 60)

                    else:
                        now = datetime.datetime.strptime(date + ' 23:59:59', '%Y-%m-%d %H:%M:%S')
                        delta = now - times[-1]
                        if delta.total_seconds() > 6 * sensor_name['frequency']:
                            lost_count += math.floor(delta.total_seconds() / sensor_name['require_frequency'])
                            _total_loss_time += math.floor(delta.total_seconds() / 60)

                            msgs.append({'status': 'red',
                                         'msg': '缺失数据时间大于' + str(
                                             math.floor(6 * sensor_name['frequency'] / 60)) + '分钟时间为,' + times[
                                                    -1].strftime('%Y-%m-%d %H:%M:%S') + ',' + now.strftime(
                                             '%Y-%m-%d %H:%M:%S') + ',' + str(
                                             math.floor(delta.total_seconds() / 60)) + ' 分钟,' + str(math.ceil(
                                             delta.total_seconds() / sensor_name['require_frequency'])) + '次\n'})

                    '''计算丢包率'''
                    suppose_count = math.ceil(
                        (now - datetime.datetime.strptime(date + ' 0:00:00', '%Y-%m-%d %H:%M:%S')).total_seconds() /
                        sensor_name['require_frequency'])
                    # if suppose_count < len(times):
                    #     suppose_count = len(times)
                    msgs.append({'status': 'red',
                                 'msg': '该设备接收记录次数为: ' + str(len(times)) + '/' + str(suppose_count) + ',丢包率为: ' + str(
                                     round((suppose_count - len(times)) / suppose_count, 4)) + '\n'})
                    msgs.append({'status': 'red',
                                 'msg': '离散丢包次数为: ' + str(_discrete_loss_times) + ',缺失时间总共为: ' + str(
                                     _total_loss_time) + '分钟'})

                else:
                    msgs.append({'status': 'red', 'msg': '数据丢失'})
                messages['时间频率'] = msgs

                try:
                    result['results'].append(
                        {'eui': sensor_name['eui'], 'info': '有分析结果', 'err_code': err_code, 'is_base':
                            threshold[sensor_name['eui']]['is_base'], 'messages': messages, 'company': key})
                except KeyError:
                    result['results'].append(
                        {'eui': sensor_name['eui'], 'info': '有分析结果', 'err_code': err_code, 'is_base': 0,
                         'messages': messages, 'company': key})

            except KeyError:
                try:
                    result['results'].append({'eui': sensor_name['eui'], 'info': '缺失当天数据\n', 'err_code': -1,
                                              'is_base': threshold[sensor_name['eui']]['is_base'],
                                              'company': key})
                except KeyError:
                    result['results'].append({'eui': sensor_name['eui'], 'info': '缺失当天数据\n', 'err_code': -1,
                                              'is_base': 0, 'company': key})
    return result


def analyze_data_time_interval(date, interval=1):
    """
    analyze lost packet within interval period one day

    :param date:
    :param interval: interval times to analyze default 60min
    :return:
    """
    df_list=[]
    df = load_sensor_group_data(date)
    df.index = pd.to_datetime(df['获取时间'])
    for key in sensor_config.keys():
        results = []
        for data in sensor_config[key]:
            sensor_data = df[df['eui'] == data['eui']]
            try:
                sensor_data = sensor_data[date]
            except Exception as e:
                print(e)
            start_time = datetime.datetime.strptime(date + ' 0:00:00', '%Y-%m-%d %H:%M:%S')
            end_time = start_time + datetime.timedelta(hours=interval)

            sensor_data['time'] = pd.to_datetime(sensor_data['获取时间'])
            receive_count = 0
            for index, item in sensor_data.iterrows():
                result = dict()

                if start_time < item['time'] < end_time:
                    receive_count += 1
                else:
                    result['eui'] = data['eui']
                    result['start_time'] = start_time
                    result['end_time'] = end_time
                    result['require_count'] = math.floor(interval * 3600 / data['require_frequency'])
                    result['received_count'] = receive_count
                    result['lost_rate'] = round((math.floor(
                        interval * 3600 / data['require_frequency']) - receive_count) / math.floor(
                        interval * 3600 / data['require_frequency']), 2)
                    start_time = end_time
                    end_time = end_time + datetime.timedelta(hours=interval)
                    receive_count = 0
                    result['interval'] = 1

                    results.append(result)
            final = dict()
            final['eui'] = data['eui']
            final['start_time'] = end_time - datetime.timedelta(hours=interval)
            final['end_time'] = end_time
            final['require_count'] = math.floor(interval * 3600 / data['require_frequency'])
            final['received_count'] = receive_count
            final['lost_rate'] = round((math.floor(
                interval * 3600 / data['require_frequency']) - receive_count) / math.floor(
                interval * 3600 / data['require_frequency']), 2)
            final['interval'] = 1
            results.append(final)

        df_result = pd.DataFrame(data=results,
                                 columns=['eui', 'start_time', 'end_time', 'require_count', 'received_count',
                                          'lost_rate', 'interval'])
        df_list.append(df_result)
        df_result.to_csv('./reports/' + key + ' ' + date + '.csv', index=False,
                         encoding='utf-8')
    return df_list


# if __name__ == '__main__':
    # df = load_data('2018-01-26')
    # print(df.head())
    # for date in ['2018-03-15', '2018-03-16', '2018-03-17']:
    #     analyze_data_time_interval(date, 1)
