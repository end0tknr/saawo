#!python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append( os.path.join(os.path.dirname(__file__), '../lib') )

from service.city       import CityService

def main():
    city_service = CityService()
    # 市区町村名、コード一覧の登録
    master_src_rows = city_service.download_master()
    city_service.del_all_tbl_rows()
    city_service.save_tbl_rows(master_src_rows)
    # google mapによる座標の登録
    city_service.calc_save_lnglat()
        
if __name__ == '__main__':
    main()
