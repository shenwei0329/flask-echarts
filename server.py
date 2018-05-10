#coding=utf-8
from __future__ import unicode_literals

from flask import Flask, render_template
import echart_handler
import handler
import pandas as pd
import datetime

app = Flask(__name__)

import numpy as np
import logging

logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)
_handler = logging.FileHandler("log.txt")
_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
_handler.setFormatter(formatter)

console = logging.StreamHandler()
console.setLevel(logging.INFO)

logger.addHandler(_handler)
logger.addHandler(console)

databases = ['CPSJ', 'FAST', 'HUBBLE', 'ROOOOT', 'RDM', 'TESTCENTER', 'JX', 'GZ']

@app.route("/")
def hello():

    # 项目统计信息
    pjStat = {
        "total": 80,
        "done": 21,
        "ing": 18,
        "pre": 41,
    }

    # 产品统计信息
    pdStat = {
        "total": 80,
        "ing": 18,
    }

    # 资源统计信息
    hrStat = {
        "total": 80,
        "done": 2000,
        "ratio": 80,
    }

    count, done_count, persion, date, cost = handler.get_task_stat()
    _persion = []
    _date = []
    _persion_max = 0
    for _p in persion:
        _val = persion[_p]
        if _val > _persion_max:
            _persion_max = _val
        _persion.append(_val)

    _today = datetime.date.today()
    _date_scale = pd.date_range(start='2018-01-01 00:00:00',
                                end='%s-%s-%s 23:59:59' % (_today.year, _today.month, _today.day),
                                freq='1D')
    pydate_array = _date_scale.to_pydatetime()
    date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(pydate_array)
    _date_scale = pd.Series(date_only_array)

    _date = []
    for _d in _date_scale:
        if _d in date:
            _date.append(date[_d])
        else:
            _date.append(0)

    # 任务统计
    taskStat = {
        "total": count,
        "done": done_count,
        "cost": cost,
        "ratio": "%0.2f" % (float(done_count*100)/float(count)),
    }

    _cost_loan = handler.get_loan_stat()
    _cost_ticket = handler.get_ticket_stat()
    # 差旅统计信息
    tripStat = {
        "total": handler.get_trip_count(),
        "loan": "%0.3f" % (_cost_loan/10000.),
        "ticket": "%0.3f" % (_cost_ticket/ 10000.),
        "totalcost": "%0.3f" % ((_cost_loan+_cost_ticket)/10000.)
    }

    context = dict(
        pjStat=pjStat,
        pdStat=pdStat,
        hrStat=hrStat,
        taskStat=taskStat,
        tripStat=tripStat,
        myechart=echart_handler.scatter3d(),
        mybarechart=echart_handler.bar(),
        mygeo=echart_handler.get_geo(),
        persionTask=echart_handler.scatter(u'【人-任务】分布', [0, _persion_max/2], _persion),
        dateTask=echart_handler.scatter(u'【日期-任务】分布', [0, _persion_max/2], _date),
        chkonam=echart_handler.scatter(u'上班时间分布', [0,12], handler.getChkOnAm('2018-04-01', '2018-04-30')),
        chkonpm=echart_handler.scatter(u'下班时间分布', [8,24], handler.getChkOnPm('2018-04-01', '2018-04-30')),
    )

    return render_template('pyecharts.html', **context)


