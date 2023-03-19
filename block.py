import json
class Block:
    def getGeojson(self, minx, miny, tile_w):
        return {
            "coordinates": [[[
                [ minx,          miny ],
                [ minx,          miny + tile_w ],
                [ minx + tile_w, miny + tile_w ],
                [ minx + tile_w, miny],
                [ minx,          miny]
            ]]],
            "type": "MultiPolygon"
        }
        
        
    def getAllGeoJson(self, filename):
        ret = {
            "type": "FeatureCollection",
            "name": "aaa",
            "crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } },
            "features": []
        }
        with open(filename, 'r') as fr:
            line = fr.readline()
            while len(line) > 0:
                tile_code, sdate, edate = line[:-1].split(' ')
                x,y = tile_code.split('_')
                minx = int(x) * 2 - 180
                miny = int(y) * 2 - 90
                
                ret['features'].append(
                    { 
                        "type": "Feature", 
                        "properties": {'tile_code': tile_code, 'sdate': sdate, "edate": edate},
                        "geometry": self.getGeojson(minx, miny, 2)
                    }
                )
                line = fr.readline()
        return ret

#with open('all_block.geojson', 'w') as fw:
#    fw.write(json.dumps(Block().getAllGeoJson('error_part4.txt')))
            
with open('final_err_block.geojson', 'w') as fw:
    fw.write(json.dumps(Block().getAllGeoJson('final_err.log')))
            