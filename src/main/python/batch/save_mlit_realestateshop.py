#!python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append( os.path.join(os.path.dirname(__file__), '../lib') )
from service.mlit_realestateshop import MlitRealEstateShopService

def main():
    real_estate_shop_service = MlitRealEstateShopService()

    # real_estate_shop_service.del_tbl_rows()
    real_estate_shop_service.download_and_save_master()

if __name__ == '__main__':
    main()
