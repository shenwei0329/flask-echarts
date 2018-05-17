#coding=utf-8
from __future__ import unicode_literals

from flask import Flask, render_template, redirect, request, make_response, abort, url_for
from flask_login import LoginManager
import flask_login
import flask_logger
import echart_handler
import handler
import pandas as pd
import datetime
import numpy as np
import logging


app = Flask(__name__)
app.secret_key = '999999'
login_manager = LoginManager()
login_manager.login_view = 'login'
login_manager.login_message = '请登陆系统！'
login_manager.session_protection = 'strong'
login_manager.init_app(app)

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
_handler = logging.FileHandler("log.txt")
_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
_handler.setFormatter(formatter)

console = logging.StreamHandler()
console.setLevel(logging.INFO)

logger.addHandler(_handler)
logger.addHandler(console)

databases = ['CPSJ', 'FAST', 'HUBBLE', 'ROOOOT', 'RDM', 'TESTCENTER', 'JX', 'GZ']
month = [u'一月', u'二月', u'三月', u'四月', u'五月', u'六月', u'七月', u'八月', u'九月', u'十月', u'十一月', u'十二月']


class User():

    def __init__(self):

        logger.warn(">>> User init <<<")
        self.username = '@all'
        self.role_id = 'user'
        self.password_hash = 'chinacloud'
        self.login_state = False
        self.active = False
        self.id = '@all'
        self.context = None

    @property
    def password(self):
        raise AttributeError('password is not a readable attribute')

    @password.setter
    def password(self, password):
        self.password_hash = password

    def verify_password(self, password):
        print(">>>[%s]<<<" % password)
        return self.password_hash == password

    def get(self, user_id):
        return self.password_hash

    def is_authenticated(self):
        logger.warn(">>> is_authenticated <<<")
        return self.login_state

    def is_active(self):
        return True

    def __repr__(self):
        return '<User %r>' % self.username


user = User()

@app.route('/index/', methods=['GET', 'POST'])
@flask_login.login_required
def main():
    logger.warn(">>> index page, method is %s" % request.method)
    return render_template('index.html', username='@all', name='@all', **set_context())


@app.route('/post')
@flask_login.login_required
def post():
    logger.warn(">>> post <<<")
    pass


@app.route("/login", methods=['GET', 'POST'])
def login():

    if request.method == 'POST':
        logger.warn(">>> login post method <<<")
        username = request.form['username']
        password = request.form['password']

        next_url = request.args.get("next")
        print('>>> next is %s' % next_url)

        if password == 'chinacloud' and username == '@all':
            logger.warn(">>> login OK! <<<")
            # set login user
            user.id = username
            user.login_state = True
            if not user.active:
                user.context = set_context()
                user.active = True
            resp = make_response(render_template('index.html', **user.context))
            resp.set_cookie('username', username)
            return redirect(next_url or url_for('main'))
        else:
            return abort(401)

    logger.warn(">>> login get method <<<")
    if user.login_state:
        return render_template('index.html', **user.context)
    return render_template('login.html')


@login_manager.user_loader
def load_user(user_id):
    logger.warn(">>> load_user <<<")
    return user.get(user_id)


@app.route('/logout')
@flask_login.login_required
def logout():
    # remove the username from the session if it's there
    logger.warn(">>> logout page <<<")
    user.login_state = False
    return redirect(url_for('login'))


def set_context():

    _today = datetime.date.today()
    _st_date = '2018-01-01'
    _ed_date = _today.strftime("%Y-%m-%d")
    _checkon_am_data, _checkon_pm_data, _checkon_work, _checkon_user, _total_work_hour = handler.getChkOn(_st_date, _ed_date)
    _act_user = 0
    for _v in _checkon_user:
        if _checkon_user[_v] > 0:
            _act_user += 1

    # 项目统计信息
    _pj_count, _pj_op, _pj_ed, _pj_ing = handler.get_pj_state()
    pjStat = {
        "total": _pj_count,
        "op": _pj_op,
        "done": _pj_ed,
        "ing": _pj_ing,
        "pre": _pj_count-_pj_ing-_pj_ed,
    }
    _contract_count, _contract_total = handler.get_contract_stat()
    _budget, _budget_list = handler.get_budget_stat()
    contractStat = {
        "count": _contract_count,
        "total": "%0.3f" % _contract_total,
        "budget": "%0.3f" % (_budget / 10000.),
        "budgeted": 0,
    }

    # 产品统计信息
    _pd_count, _pd_ing, _deliver, _deliver_count = handler.get_product_stat()
    pdStat = {
        "total": _pd_count,
        "ing": _pd_ing,
        "deliver": _deliver,
        "count": _deliver_count,
    }

    _date_scale = pd.date_range(start='2018-01-01 00:00:00',
                                end='%s-%s-%s 23:59:59' % (_today.year, _today.month, _today.day),
                                freq='1D')
    pydate_array = _date_scale.to_pydatetime()
    date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(pydate_array)
    _date_scale = pd.Series(date_only_array)

    # 资源统计信息
    persion, date, _hr_month_date = handler.get_hr_stat(_st_date, _ed_date)
    _cost = 0
    for _p in persion:
        _cost += persion[_p]

    _persion_cost = _cost/3600

    pd_count, pd_cost, pd_n_cost = handler.get_pd4pj_stat(_st_date, _ed_date)

    hrStat = {
        "cost_time": _persion_cost,
        "cost": "%0.3f" % (float(_persion_cost) * 2.5/(26. * 8.)),
        "pd_count": pd_count,
        "pd_cost_time": pd_cost,
        "pd_cost": "%0.3f" % (float(pd_cost) * 2.5 / (26. * 8.)),
        "pd_n_cost_time": pd_n_cost,
        "pd_n_cost": "%0.3f" % (float(pd_n_cost) * 2.5 / (26. * 8.)),
        "pd_cost_total": "%0.3f" % (float(pd_cost+pd_n_cost) * 2.5 / (26. * 8.)),
        "pd_cost_time_total": pd_cost + pd_n_cost,
    }

    count, done_count, persion, date, cost, g_stat = handler.get_task_stat(_st_date, _ed_date)

    _persion = []
    _persion_max = 0
    for _p in persion:
        _val = persion[_p]
        if _val > _persion_max:
            _persion_max = _val
        _persion.append(_val)

    _date = []
    for _d in _date_scale:
        if _d in date:
            _date.append(date[_d])
        else:
            _date.append(0)

    _ration = (float(len(persion))/float(_act_user))
    hrStat['ratio'] = "%0.2f" % (float(cost)*100./(_total_work_hour*_ration))

    # 任务统计
    taskStat = {
        "total": count,
        "persion_count": len(persion),
        "persion_ratio": "%0.2f" % (float(len(persion)*100)/float(_act_user)),
        "done": done_count,
        "cost_time": "%0.3f" % cost,
        "cost_base": "2.5",
        "cost": "%0.3f" % (float(cost) * 2.5/(26. * 8.)),
        "ratio": "%0.2f" % (float(done_count*100)/float(count)),

        "pd_count": g_stat['pd'][0],
        "pd_persion": g_stat['pd'][1],
        "pd_cost_time": "%0.3f" % g_stat['pd'][2],
        "pd_cost": "%0.3f" % (float(g_stat['pd'][2]) * 2.5 / (26. * 8.)),

        "pj_group": "甘孜、嘉兴、四川公安等",
        "pj_count": g_stat['pj'][0],
        "pj_persion": g_stat['pj'][1],
        "pj_cost_time": "%0.3f" % g_stat['pj'][2],
        "pj_cost": "%0.3f" % (float(g_stat['pj'][2]) * 2.5 / (26. * 8.)),

        "rdm_count": g_stat['rdm'][0],
        "rdm_persion": g_stat['rdm'][1],
        "rdm_cost_time": "%0.3f" % g_stat['rdm'][2],
        "rdm_cost": "%0.3f" % (float(g_stat['rdm'][2]) * 2.5 / (26. * 8.)),

        "count_total": g_stat['pd'][0]+g_stat['pj'][0]+g_stat['rdm'][0],
        "persion_total": g_stat['pd'][1]+g_stat['pj'][1]+g_stat['rdm'][1],
        "cost_time_total": "%0.3f" % (g_stat['pd'][2]+g_stat['pj'][2]+g_stat['rdm'][2]),
        "cost_total": "%0.3f" % (float(g_stat['pd'][2]+g_stat['pj'][2]+g_stat['rdm'][2]) * 2.5 / (26. * 8.)),

    }

    _cost_loan, _trip_month_cost = handler.get_loan_stat(_st_date, _ed_date)
    _cost_reim, _reim_month_cost = handler.get_reimbursement_stat(_st_date, _ed_date)
    _cost_ticket, _addr_data, _month_date, _month_cost = handler.get_ticket_stat(_st_date, _ed_date)
    _trip_addr_data, _trip_month_data = handler.get_trip_data(_st_date, _ed_date)
    _reim_addr_data, _reim_month_data = handler.get_reim_data(_st_date, _ed_date)
    # 差旅统计信息
    tripStat = {
        "total": handler.get_trip_count(_st_date, _ed_date),
        "loan": "%0.3f" % (_cost_loan/10000.),
        "reim": "%0.3f" % (_cost_reim/10000.),
        "ticket": "%0.3f" % (_cost_ticket/10000.),
        "totalcost": "%0.3f" % ((_cost_loan+_cost_ticket)/10000.)
    }

    # 考勤信息
    checkStat = {
        "total": len(_checkon_user),
        "ratio": "%0.2f" % (float(_act_user)*100./len(_checkon_user)),
        "actUser": _act_user,
        "workHour": _total_work_hour,
        "workHourCost": "%0.2f" % (_total_work_hour*2.5/(26.*8.)),
    }

    context = dict(
        report={"started_at": _st_date, "ended_at": _ed_date},
        user={"role": "admin"},
        pjStat=pjStat,
        contractStat=contractStat,
        pdStat=pdStat,
        hrStat=hrStat,
        taskStat=taskStat,
        tripStat=tripStat,
        checkStat=checkStat,
        planeTicket=echart_handler.get_geo(u"航程", u"数据来源于携程", _addr_data),
        hrMonth=echart_handler.bar(u'任务分布', month, [{'title': u'个数', 'data': _hr_month_date}]),
        tripMonth=echart_handler.bar(u'申请分布', month,
                                     [{'title': u'人次', 'data': _trip_month_data},
                                      {'title': u'借款金额（1000元）', 'data': _trip_month_cost}]),
        reimMonth=echart_handler.bar(u'报账分布', month,
                                     [{'title': u'人次', 'data': _reim_month_data},
                                      {'title': u'报账金额（1000元）', 'data': _reim_month_cost}]),
        planeMonth=echart_handler.bar(u'航程分布', month,
                                      [{'title': u'航次', 'data': _month_date},
                                       {'title': u'金额（1000元）', 'data': _month_cost}]),
        trip=echart_handler.get_geo(u"借款", u"信息来源于出差申请", _trip_addr_data),
        reim=echart_handler.get_geo(u"报账", u"信息来源于差旅报账申请", _reim_addr_data),
        persionTask=echart_handler.scatter(u'【人-任务】分布', [0, _persion_max/2], _persion),
        dateTask=echart_handler.scatter(u'【日期-任务】分布', [0, _persion_max/2], _date),
        chkonam=echart_handler.scatter(u'上班时间分布', [0,12], _checkon_am_data),
        chkonpm=echart_handler.scatter(u'下班时间分布', [8,24], _checkon_pm_data),
        chkonwork=echart_handler.scatter(u'工作时长分布', [0, 12], _checkon_work),
    )
    return context


