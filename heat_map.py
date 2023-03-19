from meta2pg import pg_conn
import json
from thread_util import *
from datetime import datetime
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




#####################################
## 拍摄热力图
#create table heat_map(
#    tile_code varchar,    --瓦片编号
#    year smallint,        --年份
#    l smallint,           --瓦片层级
#    c int,                --瓦片被拍摄次数
#    geom geometry         --瓦片外边框
#);




class heat_map:
    def __init__(self):
        self.level_count = {
          1: 6,   2: 6,    3: 6,    4: 6,    5: 6,    6: 6,   7: 6,
          8: 6,   9: 6,   10: 6,   11: 6,   12: 6,   13: 6,   14: 6, 15: 6, 16: 6
        }
        self.l0_w = 180
        self.heat_map_tbl = 'heat_map'
        
        self.init_sqls = (
            f'''create table if not exists {self.heat_map_tbl}(
                tile_code varchar,    --瓦片编号
                year smallint,        --年份
                l smallint,           --瓦片层级
                c int,                --瓦片被拍摄次数
                geom geometry         --瓦片外边框
            )''',
            f'create index if not exists i_tile_code on {self.heat_map_tbl}(tile_code);',
            f'create index if not exists i_year on {self.heat_map_tbl}(year);',
            f'create index if not exists i_l on {self.heat_map_tbl}(l);',
            f'create index if not exists i_geom on {self.heat_map_tbl} using gist(geom);',
        )
        
        
        
        
    def init_layer(self, start_layer, year):
        #for sql in self.init_sqls:
        #    print(sql)
        #    self.__execute_sqls__(sql)
        self.__execute_sqls__(self.init_sqls)
    
        for l in range(start_layer,start_layer+1):
            tile_w = self.l0_w/(2**l)
            sqls = list()
            for x in range(2**(l+1)):
                for y in range(2**l):
                    tile_code = f'{l}-{x}-{y}'
                    minx = tile_w * x - 180
                    miny = tile_w * y - 90
                    maxx = minx + tile_w
                    maxy = miny + tile_w
                    
                    geojson = self.getGeojson(minx, miny, tile_w)
                    
                    sqls.append(self.getInsertHeatMapSql(self.getTileCode(l, x, y), year, geojson))
            sqls.append(f'update {self.heat_map_tbl} a set c=(select count(1) from dg_scene where year={year} and st_intersects(geom, a.geom)) where l={l} and c is null')   
            self.__execute_sqls__(sqls)
            
    def getGeojson(self, minx, miny, tile_w):
        return json.dumps({
            "coordinates": [[[
                [ minx,          miny ],
                [ minx,          miny + tile_w ],
                [ minx + tile_w, miny + tile_w ],
                [ minx + tile_w, miny],
                [ minx,          miny]
            ]]],
            "type": "MultiPolygon"
        })
        
    def getTileCode(self, level, x, y):
        return f'{level}-{x}-{y}'
    
    
    def getInsertHeatMapSql(self, tile_code, year, geojson):
        (l, x, y) = tile_code.split('-')
        return f"insert into {self.heat_map_tbl}(tile_code, year, l, geom) values('{tile_code}', {year}, {l}, st_geomfromgeojson('{geojson}'))"
    ###
    ## 插入热力图表
    def insertHeatMap(self, tile_code, year, geojson):
        pg_node = pg_conn().getDBNode()
        
        with pg_node['locker']:
            cursor = None
            try:
                cursor = pg_node['conn'].cursor()
                cursor.execute(self.getInsertHeatMapSql(tile_code, year, geojson))
                pg_node['conn'].commit()
            except Exception as e:
                traceback.print_exc()
                print(e)
            finally:
                if cursor is not None:
                    cursor.close()
    
    
    def __execute_sqls__(self, sqls):
        pg_node = pg_conn().getDBNode()
        with pg_node['locker']:
            cursor = None
            try:
                cursor = pg_node['conn'].cursor()
                if type(sqls) == type(str()):
                    cursor.execute(sqls)
                else:
                    for sql in sqls:
                        cursor.execute(sql)
                pg_node['conn'].commit()
            except Exception as e:
                #traceback.print_exc()
                print(e)
            finally:
                if cursor is not None:
                    cursor.close()
        
    ###
    ##
    def getAllValidTileCode(self, level, year):
        pg_node = pg_conn().getDBNode()
        rc = list()
        
        with pg_node['locker']:
            cursor = None
            try:
                cursor = pg_node['conn'].cursor()
                
                sql = f'select distinct tile_code from {self.heat_map_tbl} where year={year} and l={level} and c>={self.level_count[level]}'
                #print(sql)
                # fetch all valid oid
                cursor.execute(sql)
                pg_node['conn'].commit()
                ret = cursor.fetchall()
                
                for i in ret:
                    rc.append(i[0])
            except Exception as e:
                #traceback.print_exc()
                print(e)
            finally:
                if cursor is not None:
                    cursor.close()
        return rc
    
    def calcTileCoverCount(self, year):
        start_layer = 4
        print('初始化表格', datetime.now().strftime("%y-%m-%d %H:%M:%S"))
        self.init_layer(start_layer, year)
        offset = 2
        thread = thread_util()
        _temp_ = 0
        print(datetime.now().strftime("%y-%m-%d %H:%M:%S"))
        for l in range(start_layer + offset, 13, offset):
            #获取上一层所有被拍摄的瓦片
            ret = self.getAllValidTileCode(l - offset, year)
            sqls = list()
            for ptile_code in ret:
                (pl, px, py) = ptile_code.split('-')
                pl = int(pl)
                px = int(px)
                py = int(py)
                l_gap = l - pl  #计算层级和上一参考层级差值
                tile_w = self.l0_w/(2**l)  #计算层级瓦片大小
                print(ptile_code)
                for x_offset in range(2**l_gap):
                    for y_offset in range(2**l_gap):
                        x = px * (2**l_gap) + x_offset
                        y = py * (2**l_gap) + y_offset
                        
                        minx = tile_w * x - 180
                        miny = tile_w * y - 90
                        maxx = minx + tile_w
                        maxy = miny + tile_w
                        
                        tile_code = f'{l}-{x}-{y}'
                        geojson = self.getGeojson(minx, miny, tile_w)
                        
                        sqls.append(self.getInsertHeatMapSql(self.getTileCode(l, x, y), year, geojson))
                        #geom = f"st_geomfromgeojson('{geojson}')"
                        #insert into xxx(tile_code, l, geom)
                        #f"select count(1) from dg_scene where year={year} and st_intersects(geom, {geom})"
                        
                if len(sqls) > 800:
                    _temp_ += 1
                    print(len(sqls))
                    sqls.append(f'update {self.heat_map_tbl} a set c=(select count(1) from dg_scene where year={year} and st_intersects(geom, a.geom)) where l={l} and c is null and year={year}' )
                    
                    thread.process(self.__execute_sqls__, (sqls.copy(),), _temp_)
                    #self.__execute_sqls__(sqls)
                    sqls.clear()
                    #sqls = list()
        
            if len(sqls) > 0:
                sqls.append(f'update {self.heat_map_tbl} a set c=(select count(1) from dg_scene where year={year} and st_intersects(geom, a.geom)) where l={l} and c is null and year={year}')
                thread.process(self.__execute_sqls__, (sqls,), _temp_)
            print(f'等待level:{l}结束', datetime.now().strftime("%y-%m-%d %H:%M:%S"))
            thread.wait()
            print(f'level:{l}结束',  datetime.now().strftime("%y-%m-%d %H:%M:%S"))
            #self.__execute_sqls__(sqls)
            #self.__execute_sqls__(f'update {self.heat_map_tbl} a set c=(select count(1) from dg_scene where year={year} and st_intersects(geom, a.geom)) where l={l} and c is null')
                
