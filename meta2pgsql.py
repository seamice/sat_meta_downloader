#!/usr/bin/python
#--* coding:utf-8 *--

import os
import json
from datetime import datetime as dt

#表格说明
#
# DROP TABLE sat_spot_scene;
# CREATE TABLE sat_spot_scene
# (
#   satelliteid character varying(30),
#   sensor character varying,
#   geom geometry,
#   satpath integer,
#   satrow integer,
#   phototime timestamp without time zone,
#   cloud integer,
#   importtime timestamp without time zone,
#   xmlfile character varying,
#   oid character varying,
#   obj_meta text,
#   context text,
#   code integer,
#   id integer,
#   th_productid character varying
# )
#
#-----------------------------------------------------
#数据库表格说明：
#-----------------------------------------------------
#  satelliteid       卫星ID[卫星名称]
#  sensor            传感器
#  geom              位置范围
#  satpath           条带path号
#  satrow            条带row号
#  phototime         摄影时间
#  cloud             云量
#  importtime        入库时间
#  xmlfile           xml文件
#  oid               object id
#  obj_meta          原始源数据状态
#  context           注释
#  code              编码【具体编码规则，查询邮件】
#  id                备用
#  th_productid      天绘产品ID
#-----------------------------------------------------

class meta2sql:
    def __init__(self, tbl):
        self.__scene_tbl__= tbl
        self.sql_prefix   = '{}{}{}{}{}'.format(
            'INSERT INTO {}(',
                'satelliteid , sensor   , geom        , satpath, ',
                'satrow      , phototime, cloud       , importtime, ',
                'xmlfile     , oid      , obj_meta    , context,',
                'code        , id       , th_productid)').format(self.__scene_tbl__)
                
        self.sql_suffix = '{}{}{}{}{}'.format(
            'VALUES(',
                "'{sat_id}' ,'{sensor}'   , {geom}    , {satpath},",
                " {satrow} ,'{phototime}', {cloud}    , {importtime},",
                "'{xmlfile}','{oid}'      ,'{obj_meta}','{context}',",
                "{code}   ,{id}       ,{th_productid});")
    
    def process(self, meta_data):
        raise 'subclass implement'


#st_geomfromgeojson   
class dgmeta2sql(meta2sql):
    def __init__(self):
        super(eval(self.__class__.__name__),self).__init__( 'dg_scene')
        
    #获取sql入库geom
    def __get_geom__(self, scene_item_data):
        #print(scene_item_data)
        geojson = dict()
        geojson['type'] = 'polygon'
        geojson['coordinates'] = scene_item_data['geometry']['rings']
        
        rc = "st_geomfromgeojson('{}')".format(json.dumps(geojson))
        return rc

    #
    def __get_sql__(self, scene_item_data):
        sql = '{}{}'.format(
            self.sql_prefix,
            self.sql_suffix.format(
                sat_id     = scene_item_data['attributes']['vehicle_name'],
                sensor     = scene_item_data['attributes']["sensor_name"],
                geom       = self.__get_geom__(scene_item_data),
                satpath    = 'null',
                satrow     = 'null',
                # 获取图像的采集时间（时间粒度为年月日）
                phototime  = dt.fromtimestamp(int(scene_item_data['attributes']['collect_time_start']) / 1000).strftime('%Y/%m/%d %H:%M:%S'),
                cloud      = scene_item_data['attributes']["area_cloud_cover_percentage"],
                importtime = 'now()',
                xmlfile    = '',
                oid        = scene_item_data['attributes']['image_identifier'],
                obj_meta   = json.dumps(scene_item_data).replace("'", "''"),
                context    = '',
                code       = 'null',
                id         = 'null',
                th_productid='null'
            )
        )
        
        rc = dict()
        rc['tbl'] = self.__scene_tbl__
        rc['sat'] = scene_item_data['attributes']['vehicle_name']
        rc['oid'] = scene_item_data['attributes']['image_identifier']
        rc['img'] = scene_item_data['attributes']['browse_url']
        rc['scene_date'] = dt.fromtimestamp(int(scene_item_data['attributes']['collect_time_start']) / 1000).strftime('%Y/%m/%d')
        rc['sql'] = sql
        
        return rc

    #处理返回的资源JSON串
    # @param json_data 从服务器获取的源数据结果
    # @return 返回结果生成的sql数列，具体格式如下：
    #    [
    #       {
    #          'sat':{卫星名称}
    #          'oid':{数据唯一ID}
    #          'img':{图片下载地址URL}
    #          'sql':{入库postgresql的sql语句}
    #          'scene_date':{影像拍摄日期}
    #       },
    #    ]
    def getSqls(self, meta_data):
        rc = list()
        if type(meta_data) == type(str()):
            json_data = json.loads(meta_data)
        else:
            json_data = meta_data
        #print(meta_data)
        #判断字符串中是否含有数据
        if json_data is None:
            return rc
        if type(json_data) != type(dict()):
            return rc
        if not json_data.__contains__('features'):
            return rc
        for item in json_data['features']:
            rc.append(self.__get_sql__(item))
        return rc

