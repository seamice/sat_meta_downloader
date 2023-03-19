from meta_db import *
import requests as req
import json
import time

from meta_down import dg_downloader

from meta2pgsql import dgmeta2sql
from meta2pg import pg_import
from heat_map import heat_map


#if __name__ == '__main__':
#    dg = dg_downloader()
#    meta_saver = meta_db('.', 'dg_ret', 'meta')
#    for x in range(135, 180):
#        for y in range(90):
#            ret = dg.getResp(x, y)
#            print(ret['tileCode'])
#            if ret['result'] != None:
#                meta_saver.save(ret['tileCode'], '', json.dumps(ret['param']), json.dumps(ret['result']))
#            else:
#                with open('error.txt', 'a+') as fw:
#                    fw.write(f"{ret['tileCode']}\n")
        
##################################################
## for dealing with error part
#if __name__ == '__main__':
#    dg = dg_downloader()
#    meta_saver = meta_db('.', 'dg_ret', 'meta')
#    
#    files = ('error_part1.txt', 'error_part2.txt', 'error_part3.txt')
#    
#    for file in files:
#        with open(file, 'r') as fr:
#            line = fr.readline()
#            while len(line) > 0:
#                (x, y) = line[:-1].split('_')
#                ret = dg.getResp(int(x), int(y))
#                print(ret['tileCode'])
#                if ret['result'] != None:
#                    meta_saver.save(ret['tileCode'], '', json.dumps(ret['param']), json.dumps(ret['result']))
#                else:
#                    with open('error.txt', 'a+') as fw:
#                        fw.write(f"{ret['tileCode']}\n")
#                line = fr.readline()




#######################################
## for insert into pg
#def meta2pg(path, db_name, tbl, meta2pg):
#    meta_saver = meta_db('.', 'dg_ret', 'meta')
#    
#    tilecodes = meta_saver.query_all_tilecode()
#    
#    
#    importor = pg_import()
#    for code in tilecodes:
#        ret = meta_saver.query_by_tilecode(code[0])
#        if ret is not None:
#            for item in ret:
#                resp_list = json.loads(item[3])
#                sqls = list()
#                for per_resp in resp_list:
#                    sqls.extend(dg.getSqls(per_resp))
#                
#                ret = importor.process(sqls)
#
#
#if __name__ == '__main__':
#
#    dgmeta2pg = dgmeta2sql()
#    meta2pg('.', 'dg_ret', 'meta', dgmeta2pg)



#######################################
## for heat_map
if __name__ == '__main__':
    heatMapIns = heat_map()
    heatMapIns.calcTileCoverCount(2018)








