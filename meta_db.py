#!/usr/bin/python3
# -*- coding: utf-8 -*-
import sys
import sqlite3
import os
import sys
import datetime as dt
import threading
import time
import random as rd


###########################################################################
# 此类主要处理原始元数据信息的存储
###############################################
# 类中数据库连接全部通过self.db_pool表示的数据库连接池进行管理，连接池中的
# 数据节点结构如下：
# 
#{
#    'db_name':'',                #数据库名称
#    'invoke_time':0,             #调用次数
#    'locker':threading.Lock(),   #数据库节点锁
#    'db_conn':None               #数据库连接
#}
class meta_db:
    # @param path     数据库存储路径
    # @param db_name  数据库名称（省略后缀）
    # @param tbl_name 数据存储表名称
    def __init__(self, path, db_name, tbl_name):
        if not hasattr(self, 'c'):
            self.c = str(rd.randint(1, 100000000))
            #self.sat_flag = sat_flag
            self.__tbl_name = tbl_name
            self.__dbpath__ = '{}/{}.db'.format(path, db_name)
            self._init_db_(self.__dbpath__, tbl_name)
    
    def _init_db_(self, dbpath, tbl_name):
        if not hasattr(self, 'db_info'):
            is_new = os.path.exists(dbpath)
            #print(dbpath, tbl_name)
            self.db_info={
                #'name': self.sat_flag,
                'locker':threading.Lock(),
                'db_conn':sqlite3.connect(dbpath, check_same_thread = False),
                'invoke_time': dt.datetime.now()
            }
            self.db_info['cursor'] = self.db_info['db_conn'].cursor()
            
            tbl_sql  = 'create table if not exists {tbl}({id}, {tcode}, {req_url}, {req_data}, {resp_data});'.format(
                tbl = tbl_name,                
                id = 'gid INTEGER PRIMARY KEY AUTOINCREMENT',
                tcode = 'tile_code varchar',
                req_url = 'req_url varchar',
                req_data= 'req_data text',
                resp_data='resp_data text')
            index_sqls = ('create index if not exists i_{0}_tcode on {0}(tile_code);'.format(tbl_name),);
            self.__execute__(tbl_sql)
            self.__execute__(index_sqls)
                    
    def __del__(self):
        with self.db_info['locker']:
            self.db_info['db_conn'].commit()
            self.db_info['cursor'].close()
            self.db_info['db_conn'].close()
            self.db_info['db_conn'] = None
    
    def query_by_tilecode(self, tile_code):
        sql = "select tile_code, req_url, req_data, resp_data from {} where tile_code='{}' order by req_url".format(self.__tbl_name, tile_code)
        return self.__query__(sql)
        
    def query_all_tilecode(self):
        sql = f"select tile_code from {self.__tbl_name}"
        return self.__query__(sql)

    #查询信息
    def __query__(self, sql):
        tw = dt.datetime.now()
        ret = tuple()
        with self.db_info['locker']:
            #idx_c = self.db_info['db_conn'].cursor() # 创建游标
            idx_c = self.db_info['cursor']
            idx_c.execute(sql)
            ret = idx_c.fetchall()
            t =dt.datetime.now() 
            if (t-self.db_info['invoke_time']).total_seconds() > 10:
                self.db_info['db_conn'].commit()
                #idx_c.close()
                #self.db_info['cursor']=self.db_info['db_conn'].cursor()
                self.db_info['invoke_time'] = t
        #print('meta_db|query    total_seconds:{:.05f}s   sql:{}'.format((dt.datetime.now()-tw).total_seconds(), sql))
        return ret

    #执行非查询sql
    def __execute__(self, sqls):
        tw = dt.datetime.now()
        sqls_data = list()
        if type(sqls) == type(str()):
            sqls_data.append(sqls)
        elif type(sqls) == type(tuple()) or type(sqls) == type(list()):
            sqls_data.extend(sqls)
        with self.db_info['locker']:
            #idx_c = self.db_info['db_conn'].cursor() # 创建游标
            idx_c = self.db_info['cursor']
            for sql in sqls_data:
                idx_c.execute(sql)
            t =dt.datetime.now() 
            if (t-self.db_info['invoke_time']).total_seconds() > 10:
                self.db_info['db_conn'].commit()
                self.db_info['invoke_time'] = t
        #print('meta_db|execute  total_seconds:{:.05f}s   sql:{}'.format((dt.datetime.now()-tw).total_seconds(), sql[:25]))
    
    ##查询信息
    #def __query__(self, sql):
    #    ret = None
    #    with self.db_info['locker']:
    #        idx_c = self.db_info['db_conn'].cursor() # 创建游标
    #        idx_c.execute(sql)
    #        ret = idx_c.fetchall()
    #        self.db_info['db_conn'].commit()
    #        idx_c.close()
    #    return ret
    #
    ##执行非查询sql
    #def __execute__(self, sqls):
    #    with self.db_info['locker']:
    #        sqls_data = list()
    #        if type(sqls) == type(str()):
    #            sqls_data.append(sqls)
    #        elif type(sqls) == type(tuple()) or type(sqls) == type(list()):
    #            sqls_data.extend(sqls)
    #        
    #        idx_c = self.db_info['db_conn'].cursor() # 创建游标
    #        for sql in sqls_data:
    #            idx_c.execute(sql)
    #        self.db_info['db_conn'].commit()
    #        idx_c.close()
    
    #存储数据
    # @param tile_code   区域编码
    # @param req_url     请求数据的URL
    # @param req_data    请求数据参数
    # @param resp_data   请求数据获取的结果集
    def save(self, tile_code, req_url, req_data, resp_data):
        rc = False
        try:
            sql = "insert into {tbl}(tile_code, req_url, req_data, resp_data) values('{tcode}', '{req_url}', '{req_data}', '{resp_data}');".format(
                tbl = self.__tbl_name,
                tcode    = tile_code,
                req_url  = req_url.replace("'", "''"),
                req_data = req_data.replace("'", "''"),
                resp_data= resp_data.replace("'", "''"));
            self.__execute__(sql)
            rc = True
        finally:
            pass
        return rc
    
