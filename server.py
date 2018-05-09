#coding=utf-8
from __future__ import unicode_literals

from pyecharts import Page
from pyecharts import Geo
import mongodb_class
from flask import Flask, render_template

mongo_db = mongodb_class.mongoDB('ext_system')
app = Flask(__name__)

@app.route("/")
def hello():
    return render_template('pyecharts.html', myechart=scatter3d(), mybarechart=bar(), mygeo=get_geo())


def get_geo():

    data = []
    addr_data = {}

    _rec = mongo_db.handler('trip_req', 'find', {u'外出类型': u'出差'})
    for _r in _rec:
        _addr = _r['起止地点'].split(u'到')
        if len(_addr) == 1:
            _addr = _r['起止地点'].split(' ')
        if len(_addr) == 1:
            _addr = _r['起止地点'].split('-')
        if len(_addr) == 1:
            _addr = _r['起止地点'].split('_')
        if len(_addr) == 1:
            _addr = _r['起止地点'].split('～')
        if len(_addr) == 1:
            _addr = _r['起止地点'].split('至')
        if len(_addr) == 1:
            if _addr[0] == u'上海嘉兴':
                _addr = [u"上海", u"嘉兴"]

        for __addr in _addr:

            __addr = __addr.replace(' ', '')

            if __addr not in addr_data:
                addr_data[__addr] = 1
            else:
                addr_data[__addr] += 1

    for _data in addr_data:
        if u'四川省厅' in _data:
            continue
        data.append((_data, addr_data[_data]),)

    print data

    geo = Geo("出差情况", "数据来自出差申请",
             title_color="#fff",
             title_pos="center",
             width=1200,
             height=600,
             background_color='#404a59')

    attr, value = geo.cast(data)

    geo.add("", attr, value, visual_range=[0, 10], visual_text_color="#fff", symbol_size=15, is_visualmap=True)
    # geo.show_config()
    return geo.render_embed()


def bar():
    from pyecharts import Bar

    # bar
    attr = ["衬衫", "羊毛衫", "雪纺衫", "裤子", "高跟鞋", "袜子"]
    v1 = [5, 20, 36, 10, 75, 90]
    v2 = [10, 25, 8, 60, 20, 80]
    bar = Bar("柱状图数据堆叠示例")
    bar.add("商家A", attr, v1, is_stack=True)
    bar.add("商家B", attr, v2, is_stack=True)
    return bar.render_embed()


def scatter3d():
    from pyecharts import Scatter3D

    import random
    data = [[random.randint(0, 100), random.randint(0, 100), random.randint(0, 100)] for _ in range(80)]
    range_color = ['#313695', '#4575b4', '#74add1', '#abd9e9', '#e0f3f8', '#ffffbf',
                   '#fee090', '#fdae61', '#f46d43', '#d73027', '#a50026']
    scatter3D = Scatter3D("3D scattering plot demo", width=1200, height=600)
    scatter3D.add("", data, is_visualmap=True, visual_range_color=range_color)
    return scatter3D.render_embed()

