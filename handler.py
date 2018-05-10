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

mongo_db = mongodb_class.mongoDB('ext_system')

db = MySQLdb.connect(host="172.16.60.2", user="tk", passwd="53ZkAuoDVc8nsrVG", db="nebula", charset='utf8')
mysql_db = mysql_hdr.SqlService(db)


def get_trip_count():
    """
    获取差旅记录个数
    :return:
    """

    # 连接ext_system数据库
    mongo_db.connect_db('ext_system')

    # 从出差申请表中获取出差类型为：出差的数据
    return mongo_db.handler('trip_req', 'find', {u'外出类型': u'出差'}).count()

def get_trip_data():
    """
    获取差旅数据，在地图上展示。
    :param mongo_db: 数据源
    :return:
    """

    addr_data = {}

    # 连接ext_system数据库
    mongo_db.connect_db('ext_system')

    # 从出差申请表中获取出差类型为：出差的数据
    _rec = mongo_db.handler('trip_req', 'find', {u'外出类型': u'出差'})
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


def getChkOnAm(st_date, ed_date):
    """
    获取员工上午到岗时间序列
    :param st_date: 起始时间
    :param ed_date: 结束时间
    :return: 到岗记录时间序列
    """
    _sql = 'select KQ_AM from checkon_t' \
           ' where str_to_date(KQ_DATE,"%%y-%%m-%%d") between "%s" and "%s"' % (st_date, ed_date)
    _res = mysql_db.do(_sql)

    _seq = ()
    for _row in _res:
        if _row[0] == '#':
            continue
        _h = calHour(_row[0])
        if (_h is None) or (_h > 12.) or (_h < 6.):
            _seq = _seq + (9.0,)
        else:
            _seq = _seq + (_h,)
    return _seq


def getChkOnPm(st_date, ed_date):
    """
    获取员工下班时间序列
    :param st_date: 起始时间
    :param ed_date: 结束时间
    :return: 下班记录时间序列
    """
    """
    _sql = 'select KQ_PM from checkon_t' + " where created_at between '%s' and '%s'" % (st_date, ed_date)
    """
    _sql = 'select KQ_PM from checkon_t ' \
           ' where str_to_date(KQ_DATE,"%%y-%%m-%%d") between "%s" and "%s"' % (st_date, ed_date)
    _res = mysql_db.do(_sql)

    _seq = ()
    for _row in _res:
        if _row[0] == '#':
            continue
        _h = calHour(_row[0])
        if (_h is None) or (_h < 12.):
            _seq = _seq + (17.5,)
        else:
            _seq = _seq + (_h,)
    return _seq


pj_list = ['CPSJ', 'FAST', 'HUBBLE', 'GZ', 'JX', 'RDM', 'ROOOT', 'TESTCENTER']


def get_task_stat():

    _count = 0
    _done_count = 0
    persion = {}
    date = {}
    _cost = 0
    for pj in pj_list:
        mongo_db.connect_db(pj)
        _rec = mongo_db.handler('issue', 'find', {"issue_type": u"任务"})
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

    return _count, _done_count, persion, date, _cost


def get_loan_stat():

    mongo_db.connect_db('ext_system')

    _rec = mongo_db.handler('loan_req', 'find', {})
    _cost = 0.
    for _r in _rec:
        _cost += float(_r[u'金额小计'])

    return _cost


def get_ticket_stat():

    mongo_db.connect_db('ext_system')

    _rec = mongo_db.handler('plane_ticket', 'find', {})
    _cost = 0.
    for _r in _rec:
        _cost += float(_r[u'实收'])

    return _cost

