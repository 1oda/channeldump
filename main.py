#!/usr/bin/env python3
# -*-coding:utf-8 -*-

"""
# File       : prom_qps.py
# Time       ：2021/07/12
# Author     ：yuetang2
# version    ：python 3.6
# Description： prometheus对接api
"""
import io
import pandas as pd
import psycopg2
import requests
import time


class Channel:
    pass

    def __init__(self, param_dic):
        self.init_param(param_dic)

    def init_param(self, param_dic):
        try:
            self.start_time = param_dic['start']
            self.end_time = param_dic['end']
            self.sub = param_dic['sub']
            self.appid = param_dic['appid'].replace(",", "|").replace(";", "|")
            self.re_type = param_dic['re_type']
            self.dc = param_dic['dc']
            self.job = param_dic['job']
            self.cs = param_dic['cs']
        except Exception as e:
            raise Exception("传参错误！ detail: %s" % e)

        if self.dc == "dx":
            self.dc_url = 'xx'
        if self.dc == "hu":
            self.dc_url = 'xx'
        if self.dc == "gz":
            self.dc_url = 'xx'
        if self.re_type == "qps":
            # self.params = 'max_over_time(QPS{appid=~"%s",channel="%s"}[1h])' % (
            #     self.appid, self.sub)
            self.params = 'max_over_time(QPS{appid=~"%s",channel="%s",' \
                          'job="%s"}[1h])' % (
                              self.appid, self.sub, self.job)
        elif self.re_type == "conc":
            self.params = 'max_over_time(Conc{appid=~"%s",channel="%s", job="%s"' \
                          '}[1h])' \
                          % (self.appid, self.sub, self.job)

    def http_prom_api(self, url, start_time, end_time, query_param):
        data = {
            'step': '3600',
            'start': start_time,
            'end': end_time,
            'query': query_param,
        }
        try:
            r = requests.get(url, params=data, timeout=10)
            # print(r.json())
            # sys.exit()
            if r.json():
                return r.json()
            else:
                exit('404')
        except:
            return

    def sum_values(self, result_data):
        value_dict = {}
        if len(result_data) > 0:
            for i in result_data:
                value = i['values']
                appid = i['metric']['appid']
                first_clock = value[0][0]
                # 查询一个appid
                if appid in value_dict.keys():
                    value_dict[appid][first_clock] = value
                # 查询多个appid
                else:
                    clock_value = {}
                    clock_value[first_clock] = value
                    value_dict[appid] = clock_value
                value_dict[appid] = dict(
                    sorted(value_dict[appid].items(), key=lambda x: x[0]))

        # 数据去重（同一appid在某一时间点可能会有多个数据）
        data_dic = {}
        for appid, appid_value in value_dict.items():
            app_data_dic = {}
            for v in appid_value.values():
                for i in v:
                    clock = i[0]
                    value = i[1]
                    app_data_dic[str(clock)] = value
            data_dic[appid] = app_data_dic

        value_dic = {}
        for appid_k, appid_v in data_dic.items():
            for clock_k, clock_v in appid_v.items():
                if clock_k not in value_dic:
                    value_dic[clock_k] = int(clock_v)
                # 查询多个appid时，将同一时间点数据累加
                else:
                    value_dic[clock_k] += int(clock_v)
        return value_dic

    def data_json(self, start_time, end_time, query_param):
        value_dicts = {}
        url = self.dc_url
        http_data = self.http_prom_api(url, start_time, end_time,
                                       query_param)
        try:
            if http_data["status"] != "success":
                print("err:" + http_data)
                return
        except Exception as e:
            print(Exception("request is err! err: %s" % e))
            return

        if http_data:
            # print(http_data)
            try:
                http_data_list = http_data["data"]["result"]
                if http_data_list:
                    dc_data = self.sum_values(http_data_list)
                else:
                    return
            except Exception as e:
                print("err: %s" % e)
            if dc_data:
                for k, v in dc_data.items():
                    if str(k) not in value_dicts:
                        value_dicts[str(k)] = int(v)
                    else:
                        value_dicts[str(k)] += int(v)
            else:
                return
        else:
            return
        return value_dicts

    def date_stamp(self, dt):
        timeArray = time.strptime(dt, "%Y-%m-%d")
        timestamp = time.mktime(timeArray)
        return int(timestamp)

    def deal_day_max_data(self):
        start_time_stamp = int(self.date_stamp(self.start_time))
        end_time_stamp = int(self.date_stamp(self.end_time))
        bett_sta_end = end_time_stamp - start_time_stamp
        if bett_sta_end < 0:
            raise Exception("时间格式错误！")
        else:
            # hours = int((end_time_stamp - start_time_stamp) / 3600)
            # data_all = {}
            data = self.data_json(start_time_stamp, end_time_stamp, self.params)
            # print(start_time_stamp)
            # print(end_time_stamp)
            # print(self.params)
        if data:
            # print(data)
            return data
        else:
            return
            # for k,v in data.items():
            #     if v > 8000:
            #         print(k,v)
            # if bett_sta_end < 0:
            #     raise Exception("时间格式错误！")
            # else:
        #         begin_time = start_time_stamp
        #         for i in range(0, hours):
        #             if begin_time == start_time_stamp:
        #                 end_time = begin_time + 3600
        #             else:
        #                 begin_time = end_time
        #                 end_time = begin_time + 3600
        #             data = self.data_json(begin_time, end_time, self.params)
        #             data_max = {}
        #             if data:
        #                 # 获取每小时的最大值
        #                 one_data = max(data.items(), key=lambda x: x[1])
        #                 data_max[one_data[0]] = one_data[1]
        #                 data_all.update(data_max)
        #             begin_time = end_time
        # else:
        #     return
        # return data_all


class Dump:
    pass

    def __init__(self, db_conn_config):
        self.db_conn_config = db_conn_config
        self.conn = None
        self._conn()

    def _conn(self):
        '''
        访问数据库
        :return: 函数执行状态，若成功返回[True, _]，若失败返回[False, exception]
        '''
        try:
            self.conn = psycopg2.connect(**self.db_conn_config)
            return True, ''
        except Exception as e:
            return False, e

    def _reConn(self, num=33, stime=10):
        '''
        尝试重连数据库，每隔10s尝试一次，总共尝试33次
        :param num: 重连次数
        :param stime: 重连间隔时间
        :return: 函数执行状态，若成功返回[True, _]，若失败返回[False, exception]
        '''
        _number = 0
        _status = True
        while _status and _number <= num:
            try:
                self.conn.ping()  # cping 校验连接是否异常
                _status = False
                return True, ''
            except:
                state, exception_info = self._conn()
                if state == True:  # 重新连接,成功退出
                    _status = False
                    return True, ''
                _number += 1
                time.sleep(stime)  # 连接不成功,休眠1min,继续循环，直到成功或重试次数结束
        return False, exception_info

    def insert(self, columns, values):
        '''
        :param sql: SQL语句
        :return:
        '''
        state, exception_info = self._reConn()

        if state == False:
            print('state == False')
        try:
            self.cursor = self.conn.cursor()
            self.cursor.copy_from(io.StringIO(values), db_config['dx'],
                                  null='',
                                  columns=columns)
            # self.cursor.execute(sql, values)
            self.conn.commit()
            self.cursor.close()
        except Exception as e:
            print(e)

    def close(self):
        '''
        断开数据库连接
        :return:
        '''
        self.conn.close()


if __name__ == '__main__':
    db_config = {
        'db_conn_config': {
            'host': 'xx',
            'user': 'xx',
            'password': 'xx',
            'dbname': 'xx',
            'port': xx
        },
        'dx': '"xxx"."xxx"'
    }

    # pram_dic = {}
    pram = []
    # pram_dic['start'] = "2020-05-13"
    # pram_dic['end'] = "2021-07-14"
    # sub = {'xx', 'xx', 'xx', 'xx', 'xx', 'xx', 'xx'}
    sub = {'xx', 'xx'}
    re_type = {'qps', 'conc'}
    # re_type = {'qps'}
    # dc = {'dx','hu','gz'}
    # hujob = {'xx', 'xx', 'xx',
    #          'x'}
    dxjob = {'x', 'x'}
    # gzjob = {'xx'}
    # cs = {'5s', '2s', 'xx', 'xx'}
    for a in sub:
        for b in re_type:
            for c in dxjob:
                pram_dic = {}
                pram_dic['start'] = "2020-05-13"
                pram_dic['end'] = "2021-07-16"
                pram_dic['sub'] = a
                pram_dic['re_type'] = b
                pram_dic['job'] = c
                # print(pram_dic)
                if pram_dic['job'] == 'xx':
                    pram_dic['cs'] = '5s'
                if pram_dic['job'] == 'xx':
                    pram_dic['cs'] = 'offline'
                # elif pram_dic['job'] == 'xx':
                #     pram_dic['cs'] = 'xx'
                # elif pram_dic['job'] == 'xx':
                #     pram_dic['cs'] = 'xx'
                # elif pram_dic['job'] == 'xx-xx':
                #     pram_dic['cs'] = 'xx'
                # print(pram_dic)
                pram.append(pram_dic)
    # print(pram)
    for i in pram:
        pram_dic = i
        # lines = file.readlines()
        with open('appid_list', 'r', encoding='utf-8') as f:
            for line in f:
                # pram_dic['sub'] = "xx"
                # pram_dic['re_type'] = "qps"
                pram_dic['appid'] = line.strip('\n')
                pram_dic['dc'] = 'dx'
                # pram_dic['job'] = 'xx'
                prom = Channel(pram_dic)
                data_dict = {}
                data_dict = prom.deal_day_max_data()
                print(line)
                print(pram_dic)
                if data_dict:
                    print(line)
                    print(pram_dic)
                    # print(data_dict)
                    for timestamp, count in data_dict.items():
                        if count < 3:
                            continue
                        # print(timestamp,count)
                        # conn = psycopg2.connect(database="xx", user="xx",
                        #                 password="xx",
                        #                 host="xxx", port="xxx")
                        # cursor = conn.cursor()
                        # print(timestamp, count)
                        columns = ("appid", "timestamp", "type", "cs",
                                   "sub", "count", "job")
                        # sql = """INSERT INTO "xx"."xx" ("appid",
                        # "timestamp", "type",  "cs", "sub", "count",  "job") VALUES (%(appid)s,%(timestamp)s, %(type)s, %(cs)s, %(sub)s, %(count)s,%(job)s)"""
                        params = {"appid": pram_dic['appid'],
                                  "timestamp": pd.Series(timestamp),
                                  "type": pram_dic['re_type'],
                                  "cs": pram_dic['cs'],
                                  "sub": pram_dic['sub'],
                                  "count": count,
                                  "job": pram_dic['job']}

                        data1 = pd.DataFrame(params)
                        # print(data1)
                        output = io.StringIO()
                        data1.to_csv(output, sep='\t', index=False,
                                     header=False)
                        output1 = output.getvalue()
                        # print(output1)
                        try:
                            dump = Dump(
                                db_conn_config=db_config['db_conn_config'])
                            dump.insert(columns=columns, values=output1)
                            dump.close()
                        except Exception as e:
                            print(e)
