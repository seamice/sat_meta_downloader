#导入数据库驱动
import sqlite3
import json
import os
import shutil
import psycopg2
import time
import threading
import traceback
pg_conf = {
    'db_info':{
        'database': 'postgres',
        'user'    : 'postgres',
        'password': '1234',
        'host'    : '127.0.0.1',
        'port'    : '5455'
    },
    'max_conn': 20
}


#CREATE TABLE dg_scene
#(
#  satelliteid character varying(30),
#  sensor character varying,
#  geom geometry,
#  satpath integer,
#  satrow integer,
#  phototime timestamp without time zone,
#  cloud integer,
#  importtime timestamp without time zone,
#  xmlfile character varying,
#  oid character varying,
#  obj_meta text,
#  context text,
#  code integer,
#  id integer,
#  th_productid character varying
#);
#
#create index i_oid on dg_scene(oid);
#create index i_geom on dg_scene using gist(geom);



class pg_conn:
    _instance_lock = threading.Lock()
    def __init__(self):
        for i in range(pg_conf['max_conn']):
            self.db_pool = list()
            db_node = {
                'locker': threading.Lock(),
                'conn': psycopg2.connect(
                    database= pg_conf['db_info']['database'], 
                    user    = pg_conf['db_info']['user'], 
                    password= pg_conf['db_info']['password'], 
                    host    = pg_conf['db_info']['host'], 
                    port    = pg_conf['db_info']['port']
                )
            }
            self.db_pool.append(db_node)
        
    def __new__(cls, *args, **kwargs):
        if not hasattr(pg_conn, "_instance"):
            with pg_conn._instance_lock:
                if not hasattr(pg_conn, "_instance"):
                    pg_conn._instance = object.__new__(cls) 
        return pg_conn._instance
        
    def getDBNode(self):
        rc = None
        while rc is None:
            for i in self.db_pool:
                if not i['locker'].locked():
                    rc = i
        return rc
    
    def __del__(self):
        for i in self.db_pool:
            with i['locker']:
                i['conn'].close()


class pg_import:
    def __init__(self):
        pass
    
    # @param data入库数据
    #  数据结构为
    # {
    #    'tbl': '入库table名称',
    #    'sqls':{
    #        'oid': '入库sql',
    #    }
    # }
    def process(self, data):
        self.insertIntoDB(data)

    def insertIntoDB(self, data):
        valid_oids = self.getValidOids(data)
        pg_node = pg_conn().getDBNode()
        valid = 0
        with pg_node['locker']:
            cursor = None
            try:
                cursor = pg_node['conn'].cursor()
                for item in data:
                    if valid_oids.__contains__(item['oid']):
                        cursor.execute(item['sql'])
                        valid += 1
                #for i in valid_oids:
                #    #print(i)
                #    valid += 1
                #    cursor.execute(data['sqls'][i])
            except Exception as e:
                print(e)
                rc = None
            finally:
                if cursor is not None:
                    pg_node['conn'].commit()
                    cursor.close()
        rc = (valid, len(data))
        return rc
    
    
    #获取所有条目中，有效的条目
    def getValidOids(self, data):
        temp_tbl = 't_temp_valid_uuid'
        pg_node = pg_conn().getDBNode()
        rc = list()
        
        with pg_node['locker']:
            cursor = None
            try:
                cursor = pg_node['conn'].cursor()
                
                sql = 'drop table if exists ' + temp_tbl + ';'
                cursor.execute(sql)
                sql = 'create temp table if not exists ' + temp_tbl + '(oid varchar, c smallint);'
                cursor.execute(sql)
                for item in data:
                    sql = "insert into {temp_tbl}(oid) values('{val}')".format(temp_tbl=temp_tbl, val=item['oid'])
                    cursor.execute(sql)
                sql = 'update {temp_tbl} a set c=(select count(1) from {tbl} where oid=a.oid);'.format(tbl=data[0]['tbl'], temp_tbl=temp_tbl)
                cursor.execute(sql)
                
                # fetch all valid oid
                cursor.execute('select oid from {} where c=0 or c is null'.format(temp_tbl))
                pg_node['conn'].commit()
                ret = cursor.fetchall()
                
                for i in ret:
                    rc.append(i[0])
            except Exception as e:
                traceback.print_exc()
                print(e)
            finally:
                if cursor is not None:
                    cursor.close()
        return rc
 