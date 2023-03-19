from meta_db import *
import requests as req
import json
import time
##
# download dg archive data from web api
# 
#
class dg_downloader:
    def __init__(self):
        self.tile_w = 2
        self.tile_h = 2
        self.url = 'https://api.discover.digitalglobe.com/v1/services/ImageServer/query'
        pass
        
    def getBoundry(self, x, y):
        return f'{x*self.tile_w-180},{y*self.tile_h-90},{(x+1)*self.tile_w-180},{(y+1)*self.tile_h-90}'
        
    def getHeader(self):
        return {
          'accept': 'application/json',
          'accept-encoding': 'gzip, deflate, br',
          'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7',
          'content-length': '538',
          'content-type': 'application/x-www-form-urlencoded',
          'origin': 'https://discover.maxar.com',
          'referer': 'https://discover.maxar.com/',
          'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
          'sec-ch-ua-mobile': '?0',
          'sec-ch-ua-platform': '"Windows"',
          'sec-fetch-dest': 'empty',
          'sec-fetch-mode': 'cors',
          'sec-fetch-site': 'cross-site',
          'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
          'x-api-key': 'iSar7CX37j2hb3Apxp7Po6i5ZDlicfkGa8voURju',
        }
    
    def getParam(self, x, y):
        return {
          'outFields': '*',
          'inSR': '4326',
          'outSR': '4326',
          'spatialRel': 'esriSpatialRelIntersects',
          'where': "sun_elevation_avg >= 0 AND image_band_name in('PAN','4-BANDS','8-BANDS','SWIR-BANDS') AND collect_time_start >= '01/01/2000 00:00:00' AND collect_time_start <= '12/31/2022 23:59:59' AND off_nadir_max <= 90",
          'returnCountOnly': 'false',
          'f': 'json',
          'geometryBasedFilters': 'area_cloud_cover_percentage <= 100',
          'geometryType': 'esriGeometryEnvelope',
          #'geometry': '115.444336,38.788345,117.762451,40.455307',
          'geometry': self.getBoundry(x, y),
          'resultRecordCount': '2000',
        }
    
    def getTileCode(self, x, y):
        return f'{x}_{y}'
    
    def getMeta(self, param):
        rc = list()
        try:
            url = self.url
            try_count = 0
            while url != None:
                resp = req.post(url, data = param, headers = self.getHeader())
                if resp.status_code == 200:
                    ret = json.loads(resp.text)
                    rc.append(ret)
                    if ret.__contains__('nextPageUrl'):
                        url = ret['nextPageUrl']
                    else:
                        url = None
                else:
                    try_count += 1
                    if try_count >= 3:
                        rc = None
                        break
                    time.sleep(1)
        except Exception as e:
            print('##################################error')
            print(e)            
            rc = None
            pass
        return rc
        
    def getResp(self, x, y):
        rc = {}
        try:
            rc['param']    = self.getParam(x,y)
            rc['tileCode'] = self.getTileCode(x,y)
            rc['result']   = self.getMeta(rc['param'])
        except Exception as e:
            pass
        return rc





