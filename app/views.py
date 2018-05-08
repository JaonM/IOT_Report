# -*- coding:utf-8 -*-
import datetime

from flask import render_template, request, jsonify

from app import app
from app.service.AnalyzeService import analyze_data_time_interval, analyze_data
from app.service.ReportService import generalize_report, insert_report, create_data_img, load_data


@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')


@app.route('/report')
def report():
    return render_template('upload.html')


@app.route('/graph')
def graph():
    return render_template('graph.html')


@app.route('/analysis')
def analysis():
    return render_template('analysis.html')


@app.route('/statistics')
def statistics():
    return render_template('statistics.html')


@app.route('/generate_report', methods=['POST'])
def generating_report():
    start_date = request.form['start_date']
    end_date = request.form['end_date']
    # start_date = '2018-03-15'
    # end_date='20018-03-15'
    print(start_date)
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    while start_date <= end_date:
        analyze_result = analyze_data(str(start_date).split()[0])
        file_name = generalize_report(str(start_date), total_result=analyze_result)
        print(file_name)
        code = insert_report(file_name, file_path='../reports/' + file_name)
        if code == -1:
            return jsonify({'code': -1, 'info': '存入数据库失败,生成终止'})
        start_date = start_date + datetime.timedelta(days=1)
    if code == 1:
        return jsonify({'code': code, 'info': '生成成功'})


@app.route('/generate_graph')
def generate_graph():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    all = []
    code = 1
    while start_date <= end_date:
        img_list = create_data_img(start_date)
        if img_list is None:
            code = -1
        all.extend(img_list)
        start_date = start_date + datetime.timedelta(days=1)
    if code == 1:
        return jsonify({'code': code, 'info': '生成成功', 'file_paths': all})


@app.route('/loss_rate_analyze')
def loss_rate_analyze():
    """
    分析丢包率
    :return:
    """
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    code = 1
    all = list()
    while start_date <= end_date:
        df_list = analyze_data_time_interval(start_date)
        start_date = start_date + datetime.timedelta(days=1)
        all.extend(df_list)
    result_list = []
    for df in all:
        result_list.extend(df.to_dict('records'))
    if len(all) == 0:
        code = -1
    return jsonify({'code': code, 'result': result_list})


@app.route('/load_data')
def load():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    page_num = request.args.get('page_num')
    result = load_data(start_date, end_date, page_num)
    code = 1
    if len(result) == 0:
        code = -1
    return {'code': code, 'result': result}


if __name__ == '__main__':
    generating_report()
