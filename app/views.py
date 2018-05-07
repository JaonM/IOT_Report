# -*- coding:utf-8 -*-
from app import app
from flask import render_template, request
import datetime
from app.service.ReportService import generalize_report, insert_report


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


@app.route('/generating_report')
def generating_report():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    # start_date = '2018-03-15'
    # end_date='20018-03-15'
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    while start_date <= end_date:
        file_name = generalize_report(start_date)
        insert_report(file_name, file_path='../reports/' + file_name)
        start_date = start_date + datetime.timedelta(days=1)


if __name__ == '__main__':
    generating_report()
