#!/usr/local/bin/python2.7
# -*- coding: utf-8 -*-
#
#   Python服务程序集
#   ================
#   2018.5.9 @Chengdu
#
#

from __future__ import unicode_literals

import mongodb_class
import MySQLdb
import mysql_hdr
import types

mongo_db = mongodb_class.mongoDB()
db = MySQLdb.connect(host="172.16.60.2", user="tk", passwd="53ZkAuoDVc8nsrVG", db="nebula", charset='utf8')
mysql_db = mysql_hdr.SqlService(db)

"""项目状态："""
pj_state = [u'在建', u'验收', u'交付', u'发布', u'运维']

pj_list = ['GZ', 'JX', 'SCGA']
rdm_list = ['RDM', 'TESTCENTER']
pd_list = ['CPSJ', 'FAST', 'HUBBLE', 'ROOOT']


def get_trip_count(st_date, ed_date):
    """
    获取差旅记录个数
    :return:
    """

    # 连接ext_system数据库
    mongo_db.connect_db('ext_system')

    # 从出差申请表中获取出差类型为：出差的数据
    return mongo_db.handler('trip_req', 'find', {u'外出类型': u'出差',
                                                 "$and": [{"审批完成时间": {"$gte": "%s" % st_date}},
                                                          {"审批完成时间": {"$lt": "%s" % ed_date}}]
                                                 }).count()


def get_trip_data(st_date, ed_date):
    """
    获取差旅数据，在地图上展示。
    :param mongo_db: 数据源
    :return:
    """

    addr_data = {}

    # 连接ext_system数据库
    mongo_db.connect_db('ext_system')

    # 从出差申请表中获取出差类型为：出差的数据
    _rec = mongo_db.handler('trip_req', 'find', {u'外出类型': u'出差',
                                                 "$and": [{"审批完成时间": {"$gte": "%s" % st_date}},
                                                          {"审批完成时间": {"$lt": "%s" % ed_date}}]})
    for _r in _rec:

        # 清洗数据，获取出差地址
        _addr = _r[u'起止地点'].split(u'到')
        if len(_addr) == 1:
            _addr = _r[u'起止地点'].split(' ')
        if len(_addr) == 1:
            _addr = _r[u'起止地点'].split('-')
        if len(_addr) == 1:
            _addr = _r[u'起止地点'].split('_')
        if len(_addr) == 1:
            _addr = _r[u'起止地点'].split('～')
        if len(_addr) == 1:
            _addr = _r[u'起止地点'].split('－')
        if len(_addr) == 1:
            _addr = _r[u'起止地点'].split('，')
        if len(_addr) == 1:
            _addr = _r[u'起止地点'].split('~')
        if len(_addr) == 1:
            _addr = _r[u'起止地点'].split('至')
        if len(_addr) == 1:
            _addr = _r[u'起止地点'].split('…')
        if len(_addr) == 1:
            _addr = _r[u'起止地点'].split('—')
        if len(_addr) == 1:
            if _addr[0] == u'上海嘉兴':
                _addr = [u"上海", u"嘉兴"]
            if _addr[0] == u'成都上海':
                _addr = [u"成都", u"上海"]
            if _addr[0] == u'成都康定':
                _addr = [u"成都", u"康定"]
            if _addr[0] == u'广西南宁':
                _addr = [u"南宁"]
            if _addr[0] == u'甘孜康定':
                _addr = [u"康定"]
            if _addr[0] == u'甘孜州康定':
                _addr = [u"康定"]
            if _addr[0] == u'天津石家庄上海':
                _addr = [u"天津", u"石家庄", u"上海"]
            if _addr[0] == u'甘孜州公安局':
                _addr = [u"康定"]

        for __addr in _addr:

            # 去掉空格信息
            __addr = __addr.replace(' ', '').\
                replace(u'康定', '').\
                replace(u'自贡', '').\
                replace(u'理塘', '').\
                replace(u'北川', '').\
                replace(u'深证', '深圳').\
                replace(u'甘孜', '康定').\
                replace(u'广西南宁', '南宁').\
                replace(u'从', '').\
                replace(u'治安', '').\
                replace(u'办公室', '').\
                replace(u'三峡博物馆', '').\
                replace(u'最高人民法院', '').\
                replace(u'，再', '').\
                replace(u'卫士通', '').\
                replace(u'联通大厦', '').\
                replace(u'四川省公安厅', u'成都').\
                replace(u'四川公安厅', u'成都').\
                replace(u'市', '')
            if len(__addr) < 2:
                continue
            if (__addr[0] == u'（') or (u'延续' in __addr):
                continue

            if __addr not in addr_data:
                addr_data[__addr] = 1
            else:
                addr_data[__addr] += 1

    # 关闭数据库
    mongo_db.close_db()

    return addr_data


def calHour(_str):
    """
    将时间字符串（HH:MM）转换成工时
    :param _str: 时间字符串
    :return: 工时数（2位小数）
    """
    ret = None
    _s = _str.split(':')
    try:
        if len(_s) == 2 and str(_s[0]).isdigit() and str(_s[1]).isdigit():
            _h = int(_s[0])
            _m = float("%.2f" % (int(_s[1])/60.))
            ret = _h + _m
    finally:
        return ret


def get_pj_state():

    _pj_op = 0
    _pj_done = 0
    _pj_ing = 0

    mongo_db.connect_db('ext_system')
    projects = mongo_db.handler('project_t', 'find', {})

    _pj_count = projects.count()
    for _pj in projects:
        if _pj[u'状态'] in [u'在建', u'验收']:
            _pj_ing += 1
        elif _pj[u'状态'] == u'交付':
            _pj_done += 1
        elif _pj[u'状态'] == u'运维':
            _pj_op += 1

    mongo_db.close_db()

    return _pj_count, _pj_op, _pj_done, _pj_ing


def getChkOn(st_date, ed_date):
    """
    获取员工上下班时间序列
    :param st_date: 起始时间
    :param ed_date: 结束时间
    :return: 到岗记录时间序列
    """
    _sql = 'select KQ_AM, KQ_PM, KQ_NAME from checkon_t' \
           ' where str_to_date(KQ_DATE,"%%y-%%m-%%d") between "%s" and "%s"' % (st_date, ed_date)
    _res = mysql_db.do(_sql)

    _seq_am = ()
    _seq_pm = ()
    _seq_work = ()
    _user = {}
    for _row in _res:
        if (_row[0] == '#') or (_row[1] == '#'):
            continue
        _h = calHour(_row[0])
        if (_h is None) or (_h > 12.) or (_h < 6.):
            _h = 9.0
            _seq_am = _seq_am + (9.0,)
        else:
            _seq_am = _seq_am + (_h,)

        _h1 = calHour(_row[1])
        if (_h1 is None) or (_h1 < 12.):
            _h1 = 17.5
            _seq_pm = _seq_pm + (17.5,)
        else:
            _seq_pm = _seq_pm + (_h1,)

        _seq_work = _seq_work + ((_h1 - _h - 0.5),)

        if _row[2] not in _user:
            _user[_row[2]] = 0
        _user[_row[2]] += 1
    return _seq_am, _seq_pm, _seq_work, _user


def get_task_stat(st_date, ed_date):

    _count = 0
    _done_count = 0
    persion = {}
    date = {}
    _cost = 0

    for pj in pd_list:
        mongo_db.connect_db(pj)
        _rec = mongo_db.handler('issue', 'find', {"issue_type": u"任务",
                                                  "$and": [{"created": {"$gte": "%s" % st_date}},
                                                           {"created": {"$lt": "%s" % ed_date}}]})
        for _r in _rec:
            _count += 1
            if _r['status'] == u'完成':
                _done_count += 1
            if _r['users'] not in persion:
                persion[_r['users']] = 0
            persion[_r['users']] += 1
            _date = _r['created'].split('T')[0]
            if _date not in date:
                date[_date] = 0
            date[_date] += 1
            if _r['spent_time'] is not None:
                _cost += float(_r['spent_time'])/3600.

        mongo_db.close_db()

    _pd_count = _count
    _pd_persion = len(persion)
    _pd_cost = _cost

    for pj in pj_list:
        mongo_db.connect_db(pj)
        _rec = mongo_db.handler('issue', 'find', {"issue_type": u"任务",
                                                  "$and": [{"created": {"$gte": "%s" % st_date}},
                                                           {"created": {"$lt": "%s" % ed_date}}]})
        for _r in _rec:
            _count += 1
            if _r['status'] == u'完成':
                _done_count += 1
            if _r['users'] not in persion:
                persion[_r['users']] = 0
            persion[_r['users']] += 1
            _date = _r['created'].split('T')[0]
            if _date not in date:
                date[_date] = 0
            date[_date] += 1
            if _r['spent_time'] is not None:
                _cost += float(_r['spent_time'])/3600.

        mongo_db.close_db()

    _pj_count = _count - _pd_count
    _pj_persion = len(persion) - _pd_persion
    _pj_cost = _cost - _pd_cost

    for pj in rdm_list:
        mongo_db.connect_db(pj)
        _rec = mongo_db.handler('issue', 'find', {"issue_type": u"任务",
                                                  "$and": [{"created": {"$gte": "%s" % st_date}},
                                                           {"created": {"$lt": "%s" % ed_date}}]})
        for _r in _rec:
            _count += 1
            if _r['status'] == u'完成':
                _done_count += 1
            if _r['users'] not in persion:
                persion[_r['users']] = 0
            persion[_r['users']] += 1
            _date = _r['created'].split('T')[0]
            if _date not in date:
                date[_date] = 0
            date[_date] += 1
            if _r['spent_time'] is not None:
                _cost += float(_r['spent_time'])/3600.

        mongo_db.close_db()

    _rdm_count = _count - _pd_count - _pj_count
    _rdm_persion = len(persion) - _pd_persion - _pj_persion
    _rdm_cost = _cost - _pd_cost - _pj_cost

    return _count, _done_count, persion, date, _cost,\
           {'pd': [_pd_count, _pd_persion, _pd_cost],
            'pj': [_pj_count, _pj_persion, _pj_cost],
            'rdm': [_rdm_count, _rdm_persion, _rdm_cost],
            }


def get_hr_stat(st_date, ed_date):

    persion = {}
    date = {}

    for pj in pd_list+pj_list+rdm_list:
        mongo_db.connect_db(pj)
        _rec = mongo_db.handler('worklog', 'find', {"$and": [{"created": {"$gte": "%s" % st_date}},
                                                             {"created": {"$lt": "%s" % ed_date}}]})
        for _r in _rec:
            if 'author' not in _r:
                continue
            if _r['author'] not in persion:
                persion[_r['author']] = 0
            persion[_r['author']] += _r['timeSpentSeconds']
            _date = _r['created'].split('T')[0]
            if _date not in date:
                date[_date] = 0
            date[_date] += 1

        mongo_db.close_db()

    return persion, date


def get_loan_stat(st_date, ed_date):

    mongo_db.connect_db('ext_system')

    _rec = mongo_db.handler('loan_req', 'find', {"$and": [{"审批完成时间": {"$gte": "%s" % st_date}},
                                                          {"审批完成时间": {"$lt": "%s" % ed_date}}]})
    _cost = 0.
    for _r in _rec:
        _cost += float(_r[u'金额小计'])

    return _cost


def get_ticket_stat(st_date, ed_date):

    mongo_db.connect_db('ext_system')

    _date = st_date.split('-')
    _st_date = u"%d年%d月%d日" % (int(_date[0]), int(_date[1]), int(_date[2]))

    _date = ed_date.split('-')
    _ed_date = u"%d年%d月%d日" % (int(_date[0]), int(_date[1]), int(_date[2]))

    _rec = mongo_db.handler('plane_ticket', 'find', {"$and": [{"起飞时间": {"$gte": "%s" % _st_date}},
                                                              {"起飞时间": {"$lt": "%s" % _ed_date}}]})
    _cost = 0.
    addr_data = {}

    for _r in _rec:
        _cost += float(_r[u'实收'])

        # 出差地址
        _addr = _r[u'航程'].split('-')

        for __addr in _addr:

            # 去掉空格信息
            if len(__addr) < 2:
                continue

            if __addr not in addr_data:
                addr_data[__addr] = 1
            else:
                addr_data[__addr] += 1

    # 关闭数据库
    mongo_db.close_db()

    return _cost, addr_data


def is_pj(pj_info, summary):

    for _pj in pj_info:

        if _pj in (u'%s' % summary.replace(u'\xa0', u' ').replace(' ', '')).upper():
            return True

    return False


def get_pd4pj_stat(st_date, ed_date):

    pj_info = {}

    mongo_db.connect_db('ext_system')
    projects = mongo_db.handler('project_t', 'find', {})

    for _pj in projects:
        if _pj[u'别名'] not in pj_info:
            pj_info[_pj[u'别名']] = [_pj[u'名称'], 0.]

    issues = []
    for _grp in ["CPSJ", "FAST", "ROOOT", "HUBBLE"]:

        """mongoDB数据库
        """
        mongo_db.connect_db(_grp)
        _search = {'issue_type': 'epic', 'summary': u'项目入侵'}
        _epic = mongo_db.handler('issue', 'find_one', _search)
        if _epic is None:
            continue

        _search = {'epic_link': _epic['issue'], 'status': u'完成',
                   "$and": [{"updated": {"$gte": "%s" % st_date}},
                            {"updated": {"$lte": "%s" % ed_date}}]}
        _cur = mongo_db.handler('issue', 'find', _search)
        for _issue in _cur:
            issues.append(_issue)
        mongo_db.close_db()

    _count = 0
    _pj_total_cost = 0.
    _npj_total_cost = 0.

    for _issue in issues:

        _it = 'spent_time'
        if type(_issue[_it]) is types.NoneType:
            continue

        _count += 1

        if not is_pj(pj_info, _issue['summary']):
            _npj_total_cost += (float(_issue['spent_time']) / 3600.)
            continue

        _pj_total_cost += (float(_issue['spent_time']) / 3600.)

    return _count, _pj_total_cost, _npj_total_cost


def get_product_stat():

    mongo_db.connect_db('ext_system')
    products = mongo_db.handler('producting_t', 'find', {u"状态": {"$not": {"$eq": u"发布"}}})
    _ing_count = products.count()
    products = mongo_db.handler('pd_shelves_t', 'find', {})
    _ed_count = products.count()

    mongo_db.close_db()

    return _ed_count, _ing_count

