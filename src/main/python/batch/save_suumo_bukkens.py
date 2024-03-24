#!python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append( os.path.join(os.path.dirname(__file__), '../lib') )
from service.suumo    import SuumoService
from service.newbuild import NewBuildService
#from service.sumstock import SumStockService

def main():
    # 物件情報の収集
    suumo_service = SuumoService()
    # suumo_service.save_bukken_infos_main()
    # suumo_service.save_bukken_details( '新築戸建',"" )
    # suumo_service.save_bukken_details( '中古戸建',"" )
    # suumo_service.save_bukken_details( '新築戸建',"shop is null" )
    # suumo_service.save_bukken_details( '中古戸建',"shop is null" )

    # 物件情報の集計 新築
    newbuild_service = NewBuildService()
    newbuild_service.calc_save_sales_count_by_shop()
    newbuild_service.calc_save_sales_count_by_shop_scale()
    newbuild_service.calc_save_sales_count_by_shop_city()
    newbuild_service.calc_save_sales_count_by_shop_town()
    newbuild_service.calc_save_sales_count_by_shop_city_scale()
    newbuild_service.calc_save_sales_count_by_city()
    newbuild_service.calc_save_sales_count_by_city_scale()
    newbuild_service.calc_save_sales_count_by_town()
    newbuild_service.calc_save_sales_count_by_town_scale()
    newbuild_service.calc_save_sales_count_by_price()
    # # 物件情報の集計 中古
    # sumstock_service = SumStockService()
    # sumstock_service.calc_save_sales_count_by_shop()
    # sumstock_service.calc_save_sales_count_by_shop_city()
    # sumstock_service.calc_save_sales_count_by_shop_town()
    # sumstock_service.calc_save_sales_count_by_city()
    # sumstock_service.calc_save_sales_count_by_town()
    # sumstock_service.calc_save_sales_count_by_price()

    
if __name__ == '__main__':
    main()
