#!python
# -*- coding: utf-8 -*-

from service.mlit_fudousantorihiki import MlitFudousanTorihikiService
from service.suumo                 import SuumoService
from util.db import Db
import appbase
import datetime
import json
import re

logger = appbase.AppBase().get_logger()

class NewBuildService(appbase.AppBase):

    def __init__(self):
        pass

    def build_type(self):
        return "新築戸建"
    
    def tbl_name_header(self):
        return "newbuild"

    def house_count(self, org_bukken):
        for atri_key in ["house_for_sale","total_house"]:
            if org_bukken[atri_key]:
                return org_bukken[atri_key]
        return 1

    def calc_sales_count_by_shop_sub(self,
                                     ret_datas_tmp,
                                     calc_key,
                                     calc_date_from,
                                     calc_date_to):
        
        suumo_service = SuumoService()
        org_bukkens = suumo_service.get_bukkens_by_check_date(
            self.build_type(),
            calc_date_from,
            calc_date_to )

        for org_bukken in org_bukkens:
            
            for pkey in ["pref","shop"]:
                if not org_bukken[pkey]:
                    org_bukken[pkey] = "?"

            pref_shop = "%s\t%s" % (org_bukken["pref"],org_bukken["shop"])

            if not pref_shop in ret_datas_tmp:
                ret_datas_tmp[pref_shop] = {
                    "onsale_page" :0,   "discuss_page" :0,
                    "onsale_count":0,   "discuss_count":0,
                    "onsale_price":0,   "discuss_price":0,
                    "onsale_days" :0,   "discuss_days" :0 }

            if not org_bukken["price"]: # 価格未公開の場合、集計対象外
                continue
            
            ret_datas_tmp[pref_shop][calc_key+"_page"]  += 1
            ret_datas_tmp[pref_shop][calc_key+"_count"] += \
                self.house_count(org_bukken)
            ret_datas_tmp[pref_shop][calc_key+"_price"] += org_bukken["price"]

            tmp_days = org_bukken["check_date"] - org_bukken["found_date"]
            ret_datas_tmp[pref_shop][calc_key+"_days"] += tmp_days.days

        return ret_datas_tmp
    
        
    def calc_sales_count_by_shop_scale_sub(self,
                                           calc_date_from,
                                           calc_date_to ):
        suumo_service = SuumoService()
        org_bukkens = suumo_service.get_bukkens_by_check_date(
            self.build_type(),
            calc_date_from,
            calc_date_to )
        
        ret_datas_tmp = {}
        for org_bukken in org_bukkens:

            for pkey in ["pref","shop"]:
                if not org_bukken[pkey]:
                    org_bukken[pkey] = "?"

            pref_shop = "%s\t%s" % (org_bukken["pref"],org_bukken["shop"])

            if not pref_shop in ret_datas_tmp:
                ret_datas_tmp[pref_shop] = {
                    "s4_points" :0,     "s4_total"  :0, "s4_onsale" :0,
                    "s9_points" :0,     "s9_total"  :0, "s9_onsale" :0,
                    "s10_points":0,     "s10_total" :0, "s10_onsale" :0 }

            if not org_bukken["total_house"] and org_bukken["house_for_sale"]:
                org_bukken["total_house"] = org_bukken["house_for_sale"]
                
            if org_bukken["total_house"] < 2:
                continue

            points_key = "s%s_points"
            total_key  = "s%s_total" 
            onsale_key = "s%s_onsale"
            
            if org_bukken["total_house"] <= 4:
                points_key = "s%s_points" % (4,)
                total_key  = "s%s_total"  % (4,)
                onsale_key = "s%s_onsale" % (4,)
            elif org_bukken["total_house"] <= 9:
                points_key = "s%s_points" % (9,)
                total_key  = "s%s_total"  % (9,)
                onsale_key = "s%s_onsale" % (9,)
            else:
                points_key = "s10_points"
                total_key  = "s10_total"
                onsale_key = "s10_onsale"

            ret_datas_tmp[pref_shop][points_key] += 1
            ret_datas_tmp[pref_shop][total_key]  += org_bukken["total_house"]
            ret_datas_tmp[pref_shop][onsale_key] += org_bukken["house_for_sale"]
        return ret_datas_tmp
    
    def calc_sales_count_by_shop_city_scale_sub(self,
                                                calc_date_from,
                                                calc_date_to ):
        suumo_service = SuumoService()
        org_bukkens = suumo_service.get_bukkens_by_check_date(
            self.build_type(),
            calc_date_from,
            calc_date_to )
        
        ret_datas_tmp = {}
        for org_bukken in org_bukkens:

            for pkey in ["pref","city","shop"]:
                if not org_bukken[pkey]:
                    org_bukken[pkey] = "?"

            pref_city_shop = "%s\t%s\t%s" % (org_bukken["pref"],
                                             org_bukken["city"],
                                             org_bukken["shop"])

            if not pref_city_shop in ret_datas_tmp:
                ret_datas_tmp[pref_city_shop] = {
                    "s4_points" :0,     "s4_total"  :0, "s4_onsale" :0,
                    "s9_points" :0,     "s9_total"  :0, "s9_onsale" :0,
                    "s10_points":0,     "s10_total" :0, "s10_onsale" :0 }

            if not org_bukken["total_house"] and org_bukken["house_for_sale"]:
                org_bukken["total_house"] = org_bukken["house_for_sale"]
                
            if org_bukken["total_house"] < 2:
                continue

            points_key = "s%s_points"
            total_key  = "s%s_total" 
            onsale_key = "s%s_onsale"
            
            if org_bukken["total_house"] <= 4:
                points_key = "s%s_points" % (4,)
                total_key  = "s%s_total"  % (4,)
                onsale_key = "s%s_onsale" % (4,)
            elif org_bukken["total_house"] <= 9:
                points_key = "s%s_points" % (9,)
                total_key  = "s%s_total"  % (9,)
                onsale_key = "s%s_onsale" % (9,)
            else:
                points_key = "s10_points"
                total_key  = "s10_total"
                onsale_key = "s10_onsale"

            ret_datas_tmp[pref_city_shop][points_key] += 1
            ret_datas_tmp[pref_city_shop][total_key] += org_bukken["total_house"]
            ret_datas_tmp[pref_city_shop][onsale_key] \
                += org_bukken["house_for_sale"]
            
        return ret_datas_tmp
    
    
    def calc_sales_count_by_city_scale_sub(self,
                                           calc_date_from,
                                           calc_date_to ):
        suumo_service = SuumoService()
        org_bukkens = suumo_service.get_bukkens_by_check_date(
            self.build_type(),
            calc_date_from,
            calc_date_to )
        
        ret_datas_tmp = {}
        for org_bukken in org_bukkens:
            
            for pkey in ["pref","city"]:
                if not org_bukken[pkey]:
                    org_bukken[pkey] = "?"

            pref_city = "%s\t%s" % (org_bukken["pref"],org_bukken["city"])

            if not pref_city in ret_datas_tmp:
                ret_datas_tmp[pref_city] = {
                    "s4_points" :0,     "s4_total"  :0, "s4_onsale" :0,
                    "s9_points" :0,     "s9_total"  :0, "s9_onsale" :0,
                    "s10_points":0,     "s10_total" :0, "s10_onsale" :0 }

            if not org_bukken["total_house"] and org_bukken["house_for_sale"]:
                org_bukken["total_house"] = org_bukken["house_for_sale"]
                
            if org_bukken["total_house"] < 2:
                continue

            points_key = "s%s_points"
            total_key  = "s%s_total" 
            onsale_key = "s%s_onsale"
            
            if org_bukken["total_house"] <= 4:
                points_key = "s%s_points" % (4,)
                total_key  = "s%s_total"  % (4,)
                onsale_key = "s%s_onsale" % (4,)
            elif org_bukken["total_house"] <= 9:
                points_key = "s%s_points" % (9,)
                total_key  = "s%s_total"  % (9,)
                onsale_key = "s%s_onsale" % (9,)
            else:
                points_key = "s10_points"
                total_key  = "s10_total"
                onsale_key = "s10_onsale"

            ret_datas_tmp[pref_city][points_key] += 1
            ret_datas_tmp[pref_city][total_key]  += org_bukken["total_house"]
            ret_datas_tmp[pref_city][onsale_key] += org_bukken["house_for_sale"]
        return ret_datas_tmp
    
    
    def calc_sales_count_by_town_scale_sub(self,
                                           calc_date_from,
                                           calc_date_to ):
        suumo_service = SuumoService()
        org_bukkens = suumo_service.get_bukkens_by_check_date(
            self.build_type(),
            calc_date_from,
            calc_date_to )
        
        # refer to https://qiita.com/acro5piano/items/e0a48905159e8a4911ab
        re_compile = re.compile("^([あ-んア-ン一-鿐]+)")

        ret_datas_tmp = {}
        for org_bukken_tmp in org_bukkens:
            org_bukken = {}
            for atri_key,atri_val in org_bukken_tmp.items():
                org_bukken[atri_key] = atri_val

            for pkey in ["pref","city"]:
                if not org_bukken[pkey]:
                    org_bukken[pkey] = "?"

            org_bukken.update({"town":org_bukken["address"]})
            re_result = re_compile.search( org_bukken["town"] )
            if re_result:
                org_bukken["town"] = re_result.group(1)
                
            pref_city_town = "%s\t%s\t%s" % (org_bukken["pref"],
                                             org_bukken["city"],
                                             org_bukken["town"] )
            
            if not pref_city_town in ret_datas_tmp:
                ret_datas_tmp[pref_city_town] = {
                    "s4_points" :0,     "s4_total"  :0, "s4_onsale" :0,
                    "s9_points" :0,     "s9_total"  :0, "s9_onsale" :0,
                    "s10_points":0,     "s10_total" :0, "s10_onsale" :0 }

            if not org_bukken["total_house"] and org_bukken["house_for_sale"]:
                org_bukken["total_house"] = org_bukken["house_for_sale"]
                
            if org_bukken["total_house"] < 2:
                continue

            points_key = "s%s_points"
            total_key  = "s%s_total" 
            onsale_key = "s%s_onsale"
            
            if org_bukken["total_house"] <= 4:
                points_key = "s%s_points" % (4,)
                total_key  = "s%s_total"  % (4,)
                onsale_key = "s%s_onsale" % (4,)
            elif org_bukken["total_house"] <= 9:
                points_key = "s%s_points" % (9,)
                total_key  = "s%s_total"  % (9,)
                onsale_key = "s%s_onsale" % (9,)
            else:
                points_key = "s10_points"
                total_key  = "s10_total"
                onsale_key = "s10_onsale"

            ret_datas_tmp[pref_city_town][points_key] += 1
            ret_datas_tmp[pref_city_town][total_key]  \
                += org_bukken["total_house"]
            ret_datas_tmp[pref_city_town][onsale_key] \
                += org_bukken["house_for_sale"]
            
        return ret_datas_tmp
    
    
    def conv_scale_sales_to_list(self,ret_datas_tmp, pkeys, calc_date ):
        ret_datas = []

        for pkeys_str, scale_sales in ret_datas_tmp.items():
            ret_data = { "calc_date" : calc_date }
            
            pkey_vals = pkeys_str.split("\t")
            i = 0
            for pkey_val in pkey_vals:
                ret_data[pkeys[i]]    = pkey_val
                scale_sales[pkeys[i]] = pkey_val
                i += 1

            ret_data["scale_sales"] = json.dumps(scale_sales, ensure_ascii=False)
            ret_datas.append( ret_data )
        return ret_datas
    
        
    def calc_sales_count_by_shop_city_sub(self,
                                          ret_datas_tmp,
                                          calc_key,
                                          calc_date_from,
                                          calc_date_to):
        suumo_service = SuumoService()
        org_bukkens = suumo_service.get_bukkens_by_check_date(
            self.build_type(),
            calc_date_from,
            calc_date_to )
        
        for org_bukken in org_bukkens:
            
            for pkey in ["pref","city","shop"]:
                if not org_bukken[pkey]:
                    org_bukken[pkey] = "?"

            pref_shop = "%s\t%s\t%s" % (org_bukken["pref"],
                                        org_bukken["city"],
                                        org_bukken["shop"])

            if not pref_shop in ret_datas_tmp:
                ret_datas_tmp[pref_shop] = {
                    "calc_date" : calc_date_to,
                    "onsale_page" :0,   "discuss_page" :0,
                    "onsale_count":0,   "discuss_count":0,
                    "onsale_price":0,   "discuss_price":0,
                    "onsale_days" :0,   "discuss_days" :0 }

            if not org_bukken["price"]: # 価格未公開の場合、集計対象外
                continue
            
            ret_datas_tmp[pref_shop][calc_key+"_page"]  += 1
            ret_datas_tmp[pref_shop][calc_key+"_count"] += \
                self.house_count(org_bukken)
            ret_datas_tmp[pref_shop][calc_key+"_price"] += org_bukken["price"]

            tmp_days = org_bukken["check_date"] - org_bukken["found_date"]
            ret_datas_tmp[pref_shop][calc_key+"_days"] += tmp_days.days

        return ret_datas_tmp
    
        
    def calc_sales_count_by_shop_town_sub(self,
                                          ret_datas_tmp,
                                          calc_key,
                                          calc_date_from,
                                          calc_date_to):
        suumo_service = SuumoService()
        org_bukkens = suumo_service.get_bukkens_by_check_date(
            self.build_type(),
            calc_date_from,
            calc_date_to )
        
        re_compile = re.compile("^([あ-んア-ン一-鿐]+)")
        
        for org_bukken_tmp in org_bukkens:
            org_bukken = {}
            for atri_key,atri_val in org_bukken_tmp.items():
                org_bukken[atri_key] = atri_val
            
            org_bukken.update({"town":org_bukken["address"]})
            re_result = re_compile.search( org_bukken["town"] )
            if re_result:
                org_bukken["town"] = re_result.group(1)
                
            for pkey in ["pref","city","town","shop"]:
                if not org_bukken[pkey]:
                    org_bukken[pkey] = "?"

            pref_shop = "\t".join([org_bukken["pref"],
                                   org_bukken["city"],
                                   org_bukken["town"],
                                   org_bukken["shop"]])
            
            if not pref_shop in ret_datas_tmp:
                ret_datas_tmp[pref_shop] = {
                    "calc_date" : calc_date_to,
                    "onsale_page" :0,   "discuss_page" :0,
                    "onsale_count":0,   "discuss_count":0,
                    "onsale_price":0,   "discuss_price":0,
                    "onsale_days" :0,   "discuss_days" :0 }

            if not org_bukken["price"]: # 価格未公開の場合、集計対象外
                continue
            
            ret_datas_tmp[pref_shop][calc_key+"_page"]  += 1
            ret_datas_tmp[pref_shop][calc_key+"_count"] += \
                self.house_count(org_bukken)
            ret_datas_tmp[pref_shop][calc_key+"_price"] += org_bukken["price"]

            tmp_days = org_bukken["check_date"] - org_bukken["found_date"]
            ret_datas_tmp[pref_shop][calc_key+"_days"] += tmp_days.days

        return ret_datas_tmp
    
        
    def calc_save_sales_count_by_shop_scale(self):
        logger.info("start")
        
        today = datetime.datetime.today().date()
        calc_date_from, calc_date_to = self.get_weekly_period(today)
        logger.info("calc period {} {}".format(calc_date_from, calc_date_to))

        ret_datas_tmp = \
            self.calc_sales_count_by_shop_scale_sub(calc_date_from,
                                                    calc_date_to)
        ret_datas = self.conv_scale_sales_to_list( ret_datas_tmp,
                                                   ["pref","shop"],
                                                   calc_date_to )
        util_db = Db()
        util_db.bulk_upsert(
            self.tbl_name_header()+"_sales_count_by_shop_scale",
            ["pref","shop","calc_date"],
            ["pref","shop","calc_date","scale_sales"],
            ["scale_sales"],
            ret_datas )
        
        return ret_datas

    def calc_save_sales_count_by_shop_city_scale(self):
        logger.info("start")
        
        today = datetime.datetime.today().date()
        calc_date_from, calc_date_to = self.get_weekly_period(today)

        ret_datas_tmp = self.calc_sales_count_by_shop_city_scale_sub(
            calc_date_from,
            calc_date_to)
        ret_datas = self.conv_scale_sales_to_list( ret_datas_tmp,
                                                   ["pref","city","shop"],
                                                   calc_date_to )
        util_db = Db()
        util_db.bulk_upsert(
            self.tbl_name_header()+"_sales_count_by_shop_city_scale",
            ["pref","city","shop","calc_date"],
            ["pref","city","shop","calc_date","scale_sales"],
            ["scale_sales"],
            ret_datas )
        
        return ret_datas

    
    def calc_save_sales_count_by_city_scale(self):
        logger.info("start")
        
        today = datetime.datetime.today().date()
        calc_date_from, calc_date_to = self.get_weekly_period(today)

        ret_datas_tmp = self.calc_sales_count_by_city_scale_sub(calc_date_from,
                                                                calc_date_to)
        ret_datas = self.conv_scale_sales_to_list( ret_datas_tmp,
                                                   ["pref","city"],
                                                   calc_date_to )
        util_db = Db()
        util_db.bulk_upsert(
            self.tbl_name_header()+"_sales_count_by_city_scale",
            ["pref","city","calc_date"],
            ["pref","city","calc_date","scale_sales"],
            ["scale_sales"],
            ret_datas )
        
        return ret_datas

    
    def calc_save_sales_count_by_town_scale(self):
        logger.info("start")
        
        today = datetime.datetime.today().date()
        calc_date_from, calc_date_to = self.get_weekly_period(today)

        ret_datas_tmp = self.calc_sales_count_by_town_scale_sub(calc_date_from,
                                                                calc_date_to)
        ret_datas = self.conv_scale_sales_to_list( ret_datas_tmp,
                                                   ["pref","city","town"],
                                                   calc_date_to )
        util_db = Db()
        util_db.bulk_upsert(
            self.tbl_name_header()+"_sales_count_by_town_scale",
            ["pref","city","town","calc_date"],
            ["pref","city","town","calc_date","scale_sales"],
            ["scale_sales"],
            ret_datas )
        
        return ret_datas

    
    def calc_save_sales_count_by_shop(self):
        logger.info("start")
        
        today = datetime.datetime.today().date()
        
        calc_date_from, calc_date_to = self.get_weekly_period(today)
        logger.info("calc period {} {}".format(calc_date_from, calc_date_to))

        ret_datas_tmp = self.calc_sales_count_by_shop_sub({},
                                                          "onsale",
                                                          calc_date_from,
                                                          calc_date_to)
        pre_calc_date_from, pre_calc_date_to = \
            self.get_weekly_period( today - datetime.timedelta(days=7) )
        logger.info("calc pre-period {} {}".format(pre_calc_date_from, pre_calc_date_to))

        ret_datas_tmp = self.calc_sales_count_by_shop_sub(ret_datas_tmp,
                                                          "discuss",
                                                          pre_calc_date_from,
                                                          pre_calc_date_to)
        ret_datas = []
        for pref_shop, shop_info in ret_datas_tmp.items():
            (shop_info["pref"],shop_info["shop"]) = pref_shop.split("\t")

            # postgresはdate型の制約が厳しいのですが
            # pythonが内部的に datetime.date(2022, 7, 3) のようにcastしれくれます
            shop_info["calc_date"]  = calc_date_to

            for calc_key in ["onsale","discuss"]:
                page_key  = calc_key+"_page"
                count_key = calc_key+"_count"
                price_key = calc_key+"_price"
                days_key  = calc_key+"_days"
                
                tmp_size = 0
                if page_key in shop_info and shop_info[page_key]:
                    tmp_size = shop_info[page_key]
                elif shop_info[count_key]:
                    tmp_size = shop_info[count_key]

                if not tmp_size:
                    continue

                avg_price = shop_info[price_key] / tmp_size
                avg_days  = shop_info[days_key]  / tmp_size

                shop_info[price_key] = avg_price
                shop_info[days_key]  = avg_days

            ret_datas.append(shop_info)

        util_db = Db()
        util_db.bulk_upsert(
            self.tbl_name_header()+"_sales_count_by_shop",
            ["pref","shop","calc_date"],
            ["pref","shop","calc_date",
             "discuss_count",   "discuss_price",   "discuss_days",
             "onsale_count","onsale_price","onsale_days"],
            ["discuss_count",   "discuss_price",   "discuss_days",
             "onsale_count","onsale_price","onsale_days"],
            ret_datas )
        
        return ret_datas

    def calc_save_sales_count_by_shop_town(self):
        logger.info("start")
        
        today = datetime.datetime.today().date()
        calc_date_from, calc_date_to = self.get_weekly_period(today)

        ret_datas_tmp = self.calc_sales_count_by_shop_town_sub({},
                                                               "onsale",
                                                               calc_date_from,
                                                               calc_date_to)
        
        pre_calc_date_from, pre_calc_date_to = \
            self.get_weekly_period( today - datetime.timedelta(days=7) )

        ret_datas_tmp = self.calc_sales_count_by_shop_town_sub(ret_datas_tmp,
                                                               "discuss",
                                                               pre_calc_date_from,
                                                               pre_calc_date_to)
        
        ret_datas = []
        for pref_shop, shop_info in ret_datas_tmp.items():
            (shop_info["pref"],
             shop_info["city"],
             shop_info["town"],
             shop_info["shop"]) = pref_shop.split("\t")

            # postgresはdate型の制約が厳しいのですが
            # pythonが内部的に datetime.date(2022, 7, 3) のようにcastしれくれます
            shop_info["calc_date"]  = calc_date_to
            
            for calc_key in ["onsale","discuss"]:
                page_key  = calc_key+"_page"
                count_key = calc_key+"_count"
                price_key = calc_key+"_price"
                days_key  = calc_key+"_days"
                
                tmp_size = 0
                if page_key in shop_info and shop_info[page_key]:
                    tmp_size = shop_info[page_key]
                elif shop_info[count_key]:
                    tmp_size = shop_info[count_key]

                if not tmp_size:
                    continue

                avg_price = shop_info[price_key] / tmp_size
                avg_days  = shop_info[days_key]  / tmp_size
                shop_info[price_key] = avg_price
                shop_info[days_key]  = avg_days
                
            ret_datas.append(shop_info)

        util_db = Db()
        util_db.bulk_upsert(
            self.tbl_name_header()+"_sales_count_by_shop_town",
            ["pref","city","town","shop","calc_date"],
            ["pref","city","town","shop","calc_date",
             "discuss_count",   "discuss_price",   "discuss_days",
             "onsale_count","onsale_price","onsale_days"],
            ["discuss_count",   "discuss_price",   "discuss_days",
             "onsale_count","onsale_price","onsale_days"],
            ret_datas )
        
        return ret_datas

    def calc_save_sales_count_by_shop_city(self):
        logger.info("start")
        
        today = datetime.datetime.today().date()
        calc_date_from, calc_date_to = self.get_weekly_period(today)

        ret_datas_tmp = self.calc_sales_count_by_shop_city_sub({},
                                                               "onsale",
                                                               calc_date_from,
                                                               calc_date_to)
        
        pre_calc_date_from, pre_calc_date_to = \
            self.get_weekly_period( today - datetime.timedelta(days=7) )

        ret_datas_tmp = self.calc_sales_count_by_shop_city_sub(ret_datas_tmp,
                                                               "discuss",
                                                               pre_calc_date_from,
                                                               pre_calc_date_to)
        
        ret_datas = []
        for pref_shop, shop_info in ret_datas_tmp.items():
            (shop_info["pref"],shop_info["city"],shop_info["shop"]) = \
                pref_shop.split("\t")

            # postgresはdate型の制約が厳しいのですが
            # pythonが内部的に datetime.date(2022, 7, 3) のようにcastしれくれます
            shop_info["calc_date"]  = calc_date_to
            
            for calc_key in ["onsale","discuss"]:
                page_key  = calc_key+"_page"
                count_key = calc_key+"_count"
                price_key = calc_key+"_price"
                days_key  = calc_key+"_days"
                
                tmp_size = 0
                if page_key in shop_info and shop_info[page_key]:
                    tmp_size = shop_info[page_key]
                elif shop_info[count_key]:
                    tmp_size = shop_info[count_key]

                if not tmp_size:
                    continue

                avg_price = shop_info[price_key] / tmp_size
                avg_days  = shop_info[days_key]  / tmp_size
                shop_info[price_key] = avg_price
                shop_info[days_key]  = avg_days
                
            ret_datas.append(shop_info)

        util_db = Db()
        util_db.bulk_upsert(
            self.tbl_name_header()+"_sales_count_by_shop_city",
            ["pref","city","shop","calc_date"],
            ["pref","city","shop","calc_date",
             "discuss_count",   "discuss_price",   "discuss_days",
             "onsale_count","onsale_price","onsale_days"],
            ["discuss_count",   "discuss_price",   "discuss_days",
             "onsale_count","onsale_price","onsale_days"],
            ret_datas )
        
        return ret_datas

    def calc_save_sales_count_by_city(self):
        logger.info("start")
        
        today = datetime.datetime.today().date()
        calc_date_from, calc_date_to = self.get_weekly_period(today)
        ret_datas_tmp = self.calc_sales_count_by_city_sub({},
                                                          "onsale",
                                                          calc_date_from,
                                                          calc_date_to)
        
        pre_calc_date_from, pre_calc_date_to = \
            self.get_weekly_period( today - datetime.timedelta(days=7) )

        ret_datas_tmp = self.calc_sales_count_by_city_sub(ret_datas_tmp,
                                                          "discuss",
                                                          pre_calc_date_from,
                                                          pre_calc_date_to)
        
        ret_datas_tmp = self.calc_sold_count_by_city_sub(ret_datas_tmp,
                                                         calc_date_to)
        
        ret_datas = []
        for pref_city, city_info in ret_datas_tmp.items():
            
            (city_info["pref"],city_info["city"]) = pref_city.split("\t")
            city_info["calc_date"]  = calc_date_to
            
            for calc_key in ["onsale","discuss","sold"]:
                page_key  = calc_key+"_page"
                count_key = calc_key+"_count"
                price_key = calc_key+"_price"
                days_key  = calc_key+"_days"

                tmp_size = 0
                if page_key in city_info and city_info[page_key]:
                    tmp_size = city_info[page_key]
                elif city_info[count_key]:
                    tmp_size = city_info[count_key]

                if not tmp_size:
                    continue

                if calc_key != "sold":
                    avg_price = city_info[price_key] / tmp_size
                    city_info[price_key] = avg_price
                
                    avg_days  = city_info[days_key] / tmp_size
                    city_info[days_key]  = avg_days
                

            ret_datas.append(city_info)

        util_db = Db()
        util_db.bulk_upsert(
            self.tbl_name_header()+"_sales_count_by_city",
            ["pref","city","calc_date"],
            ["pref","city","calc_date",
             "discuss_count", "discuss_price", "discuss_days",
             "onsale_count",  "onsale_price",  "onsale_days",
             "sold_count",    "sold_price"],
            ["discuss_count", "discuss_price", "discuss_days",
             "onsale_count",  "onsale_price",  "onsale_days",
             "sold_count",    "sold_price"],
            ret_datas )
        
        return ret_datas

    def calc_sales_count_by_city_sub(self,
                                     ret_datas_tmp,
                                     calc_key,
                                     calc_date_from,
                                     calc_date_to):
        
        suumo_service = SuumoService()
        org_bukkens = suumo_service.get_bukkens_by_check_date(
            self.build_type(),
            calc_date_from,
            calc_date_to )
        
        for org_bukken in org_bukkens:
            
            for pkey in ["pref","city"]:
                if not org_bukken[pkey]:
                    org_bukken[pkey] = "?"

            pref_city = "%s\t%s" % (org_bukken["pref"],org_bukken["city"])

            if not pref_city in ret_datas_tmp:
                ret_datas_tmp[pref_city] = {
                    "calc_date" : calc_date_to,
                    "onsale_page" : 0, "discuss_page" :0,
                    "onsale_count" :0, "onsale_price" :0, "onsale_days" :0,
                    "discuss_count":0, "discuss_price":0, "discuss_days":0,
                    "sold_count"   :0, "sold_price"   :0}


            if not org_bukken["price"]: # 価格未公開の場合、集計対象外
                continue
            
            ret_datas_tmp[pref_city][calc_key+"_page"]  += 1
            ret_datas_tmp[pref_city][calc_key+"_count"] \
                += self.house_count(org_bukken)
            
            ret_datas_tmp[pref_city][calc_key+"_price"] += org_bukken["price"]

            tmp_days = org_bukken["check_date"] - org_bukken["found_date"]
            ret_datas_tmp[pref_city][calc_key+"_days"] += tmp_days.days

        return ret_datas_tmp

    def conv_date_to_year_quatar(self, date_org):
        if date_org.month in [1,2,3]:
            return int( "%s%s" % (date_org.year-1, 4) )

        quatar = int( date_org.month / 3 )
        return int( "%s%s" % (date_org.year, quatar) )
    
        
    def calc_sold_count_by_city_sub(self,
                                    ret_datas_tmp,
                                    calc_date_to):

        year_quatars = []       # 直近から3Q分
        date_tmp = calc_date_to
        while len(year_quatars) < 3:
            year_quatar = self.conv_date_to_year_quatar( date_tmp )
            if not year_quatar in year_quatars:
                year_quatars.append( year_quatar )
            date_tmp = date_tmp - datetime.timedelta(days=30)
        year_quatars.reverse() # 降順化

        # 直近から3Q以内で最も新しい summaryを取得
        fudousantorihiki_service = MlitFudousanTorihikiService()
        sold_summaries = \
            fudousantorihiki_service.get_city_quarters(self.tbl_name_header(),
                                                       year_quatars )
        for sold_summary in sold_summaries:
            pref_city = sold_summary["pref"]+"\t"+sold_summary["city"]
            
            if not pref_city in ret_datas_tmp:
                ret_datas_tmp[pref_city] = {
                    "onsale_count" :0, "onsale_price" :0, "onsale_days" :0,
                    "discuss_count":0, "discuss_price":0, "discuss_days":0,
                    "sold_count"   :0, "sold_price"    :0}
            
            ret_datas_tmp[pref_city]["sold_count"] = sold_summary["sold_count"]
            ret_datas_tmp[pref_city]["sold_price"] = sold_summary["sold_price"]
            
        return ret_datas_tmp
    
    
    def calc_sold_count_by_town_sub(self,
                                    ret_datas_tmp,
                                    calc_date_to):

        year_quatars = []
        date_tmp = calc_date_to
        while len(year_quatars) < 3:
            year_quatar = self.conv_date_to_year_quatar( date_tmp )
            if not year_quatar in year_quatars:
                year_quatars.append( year_quatar )
            date_tmp = date_tmp - datetime.timedelta(days=30)
        year_quatars.reverse() # 降順化
        
        fudousantorihiki_service = MlitFudousanTorihikiService()
        sold_summaries = \
            fudousantorihiki_service.get_town_quarters(self.tbl_name_header(),
                                                        year_quatars )
        for sold_summary in sold_summaries:
            pref_city_town = "%s\t%s\t%s" % (sold_summary["pref"],
                                             sold_summary["city"],
                                             sold_summary["town"] )
            
            if not pref_city_town in ret_datas_tmp:
                ret_datas_tmp[pref_city_town] = {
                    "discuss_count":0, "discuss_price":0, "discuss_days":0,
                    "onsale_count" :0, "onsale_price" :0, "onsale_days" :0,
                    "sold_count"   :0, "sold_price"    :0}
            
            ret_datas_tmp[pref_city_town]["sold_count"] = \
                sold_summary["sold_count"]
            ret_datas_tmp[pref_city_town]["sold_price"] = \
                sold_summary["sold_price"]
            
        return ret_datas_tmp
    

    def calc_sold_count_by_price_sub(self,
                                    ret_datas_tmp,
                                    calc_date_to):
        year_quatars = []
        date_tmp = calc_date_to
        while len(year_quatars) < 3:
            year_quatar = self.conv_date_to_year_quatar( date_tmp )
            if not year_quatar in year_quatars:
                year_quatars.append( year_quatar )
            date_tmp = date_tmp - datetime.timedelta(days=30)
        year_quatars.reverse() # 降順化
        
        fudousantorihiki_service = MlitFudousanTorihikiService()
        sold_summaries = \
            fudousantorihiki_service.get_city_price_summaries(
                self.tbl_name_header(), year_quatars )
        
        re_compile = re.compile("m_yen_(\d+)")

        for sold_summary in sold_summaries:

            pref_city_price = "\t".join([sold_summary["pref"],
                                         sold_summary["city"],
                                         sold_summary["price"] ])

            if not pref_city_price in ret_datas_tmp:
                ret_datas_tmp[pref_city_price] = {
                    "discuss_count":0, "discuss_days":0,
                    "onsale_count" :0, "onsale_days" :0,
                    "sold_count"   :0, "sold_count_q":0}

            # 3ケ月計を週の値に除算
            ret_datas_tmp[pref_city_price]["sold_count"] = \
                round(sold_summary["sold_count"]/12,2)
            ret_datas_tmp[pref_city_price]["sold_count_q"] = \
                sold_summary["sold_count"]

        return ret_datas_tmp
    
    
    def get_weekly_period(self,today):
        weekday = today.weekday() # 0=Mon ... 6=Sun
        calc_date_from = today - datetime.timedelta(days= weekday  ) # Mon
        calc_date_to   = today + datetime.timedelta(days= 6-weekday) # Sun
        return calc_date_from, calc_date_to
    

    def get_newest_sales_count_by_city(self):
        sql ="""
SELECT *
FROM newbuild_sales_count_by_city
WHERE calc_date=(SELECT max(calc_date) FROM newbuild_sales_count_by_city)
"""
        db_conn = self.db_connect()
        ret_datas = []
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute(sql)
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return []
            
            for ret_row in  db_cur.fetchall():
                ret_datas.append( dict( ret_row ))
        return ret_datas
    
    def get_newest_sales_count_by_town(self):
        sql ="""
SELECT tbl1.*
FROM newbuild_sales_count_by_town tbl1
WHERE calc_date=(SELECT max(calc_date) FROM newbuild_sales_count_by_town)
"""
        db_conn = self.db_connect()
        ret_datas = []
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute(sql)
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return []
            
            for ret_row in  db_cur.fetchall():
                ret_datas.append( dict( ret_row ))
        return ret_datas
    
    def get_newest_sales_count_by_shop_city(self):
        sql ="""
SELECT pref, city, count(shop) as shop
FROM newbuild_sales_count_by_shop_city
WHERE calc_date=(SELECT max(calc_date)
                 FROM newbuild_sales_count_by_shop_city)
GROUP BY pref, city
ORDER BY pref, city
"""
        db_conn = self.db_connect()
        ret_datas = []
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute(sql)
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return []
            
            for ret_row in  db_cur.fetchall():
                ret_datas.append( dict( ret_row ))
        return ret_datas
    
    def get_newest_sales_count_by_shop_town(self):
        sql ="""
SELECT pref, city, town, count(shop) as shop
FROM newbuild_sales_count_by_shop_town
WHERE calc_date=(SELECT max(calc_date)
                 FROM newbuild_sales_count_by_shop_town)
GROUP BY pref, city, town
ORDER BY pref, city, town
"""
        db_conn = self.db_connect()
        ret_datas = []
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute(sql)
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return []
            
            for ret_row in  db_cur.fetchall():
                ret_datas.append( dict( ret_row ))
        return ret_datas
    

    def get_all_town_names(self):
        sql ="""
SELECT pref,city,town
FROM newbuild_sales_count_by_town
GROUP BY pref,city,town
ORDER BY pref,city,town
"""
        db_conn = self.db_connect()
        ret_datas = []
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute(sql)
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return []
            
            for ret_row in  db_cur.fetchall():
                ret_datas.append( dict( ret_row ))
        return ret_datas
        
        
    def calc_save_sales_count_by_town(self):
        logger.info("start")
        
        today = datetime.datetime.today().date()
        calc_date_from, calc_date_to = self.get_weekly_period(today)

        ret_datas_tmp = self.calc_sales_count_by_town_sub({},
                                                          "onsale",
                                                          calc_date_from,
                                                          calc_date_to)
        
        pre_calc_date_from, pre_calc_date_to = \
            self.get_weekly_period( today - datetime.timedelta(days=7) )

        ret_datas_tmp = self.calc_sales_count_by_town_sub(ret_datas_tmp,
                                                          "discuss",
                                                          pre_calc_date_from,
                                                          pre_calc_date_to)
        
        ret_datas_tmp = self.calc_sold_count_by_town_sub(ret_datas_tmp,
                                                         calc_date_to)
        
        ret_datas = []
        for pref_city_town, town_info in ret_datas_tmp.items():
            (town_info["pref"],town_info["city"],town_info["town"]) = \
                pref_city_town.split("\t")

            # postgresはdate型の制約が厳しいのですが
            # pythonが内部的にdatetime.date(2022, 7, 3) のようにcastしれくれます
            town_info["calc_date"]  = calc_date_to
            
            for calc_key in ["onsale","discuss","sold"]:
                page_key  = calc_key+"_page"
                count_key = calc_key+"_count"
                price_key = calc_key+"_price"
                days_key  = calc_key+"_days"
                
                tmp_size = 0
                if page_key in town_info and town_info[page_key]:
                    tmp_size = town_info[page_key]
                elif town_info[count_key]:
                    tmp_size = town_info[count_key]

                if not tmp_size:
                    continue

                if calc_key != "sold":
                    avg_price = town_info[price_key] / tmp_size
                    town_info[price_key] = avg_price
                
                    avg_days  = town_info[days_key] / tmp_size
                    town_info[days_key]  = avg_days

            ret_datas.append(town_info)

        util_db = Db()
        util_db.bulk_upsert(
            self.tbl_name_header()+"_sales_count_by_town",
            ["pref","city","town","calc_date"],
            ["pref","city","town","calc_date",
             "discuss_count", "discuss_price", "discuss_days",
             "onsale_count",  "onsale_price",  "onsale_days",
             "sold_count",    "sold_price"],
            ["discuss_count", "discuss_price", "discuss_days",
             "onsale_count",  "onsale_price",  "onsale_days",
             "sold_count",    "sold_price"],
            ret_datas )
        
        return ret_datas

    def calc_sales_count_by_town_sub(self,
                                     ret_datas_tmp,
                                     calc_key,
                                     calc_date_from,
                                     calc_date_to):
        
        suumo_service = SuumoService()
        org_bukkens = suumo_service.get_bukkens_by_check_date(
            self.build_type(),
            calc_date_from,
            calc_date_to )
        
        # refer to https://qiita.com/acro5piano/items/e0a48905159e8a4911ab
        re_compile = re.compile("^([あ-んア-ン一-鿐]+)")

        for org_bukken in org_bukkens:
            
            for pkey in ["pref","city"]:
                if not org_bukken[pkey]:
                    org_bukken[pkey] = "?"

            town = org_bukken["address"]
            re_result = re_compile.search( town )
            if re_result:
                town = re_result.group(1)

            pref_city_town = "%s\t%s\t%s" % (org_bukken["pref"],
                                             org_bukken["city"],
                                             town)
            
            if not pref_city_town in ret_datas_tmp:
                ret_datas_tmp[pref_city_town] = {
                    "calc_date" : calc_date_to,
                    "onsale_page"  :0, "discuss_page" :0,
                    "onsale_count" :0, "onsale_price" :0, "onsale_days" :0,
                    "discuss_count":0, "discuss_price":0, "discuss_days":0,
                    "sold_count"   :0, "sold_price"   :0 }

            if not org_bukken["price"]: # 価格未公開の場合、集計対象外
                continue
            
            ret_datas_tmp[pref_city_town][calc_key+"_page"]  += 1
            ret_datas_tmp[pref_city_town][calc_key+"_count"] \
                += self.house_count(org_bukken)
            ret_datas_tmp[pref_city_town][calc_key+"_price"] \
                += org_bukken["price"]
            tmp_days = org_bukken["check_date"] - org_bukken["found_date"]
            ret_datas_tmp[pref_city_town][calc_key+"_days"] += tmp_days.days

        return ret_datas_tmp

    
    def calc_save_sales_count_by_price(self):
        logger.info("start")
        
        today = datetime.datetime.today().date()
        calc_date_from, calc_date_to = self.get_weekly_period(today)

        ret_datas_tmp = \
            self.calc_sales_count_by_city_price_sub({},
                                                    "onsale",
                                                    calc_date_from,
                                                    calc_date_to)
        pre_calc_date_from, pre_calc_date_to = \
            self.get_weekly_period( today - datetime.timedelta(days=7) )

        ret_datas_tmp = \
            self.calc_sales_count_by_city_price_sub(ret_datas_tmp,
                                                    "discuss",
                                                    pre_calc_date_from,
                                                    pre_calc_date_to)
        
        ret_datas_tmp = self.calc_sold_count_by_price_sub(ret_datas_tmp,
                                                          calc_date_to)

        ret_datas = []
        for pref_city_price, city_price_info in ret_datas_tmp.items():
            
            (city_price_info["pref"],
             city_price_info["city"],
             city_price_info["price"]) = pref_city_price.split("\t")
            
            # postgresの型制限(制約?)が厳しい為
            city_price_info["price"] = float( city_price_info["price"] )

            # city_price_info["sold_count_q"] = city_price_info["sold_count"]
            # # 3ケ月計の値を週次の値に
            # city_price_info["sold_count"] = \
            #     round(city_price_info["sold_count"] / 12, 2)

            # postgresはdate型の制約が厳しいのですが
            # pythonが内部的に、datetime.date(2022,7,3) のようにcastしれくれます
            city_price_info["calc_date"]  = calc_date_to

            for calc_key in ["onsale","discuss","sold"]:
                count_key = calc_key+"_count"
                days_key  = calc_key+"_days"
                
                tmp_size = 0
                if city_price_info[count_key]:
                    tmp_size = city_price_info[count_key]

                if not tmp_size:
                    continue

                if calc_key != "sold":
                    avg_days  = city_price_info[days_key] / tmp_size
                    city_price_info[days_key]  = avg_days
                    
            if not "sold_count_q" in city_price_info:
                city_price_info["sold_count_q"] = 0

            ret_datas.append(city_price_info)

        util_db = Db()
        util_db.bulk_upsert(
            self.tbl_name_header()+"_sales_count_by_city_price",
            ["pref","city","price","calc_date"],
            ["pref","city","price","calc_date",
             "discuss_count", "discuss_days",
             "onsale_count",  "onsale_days",
             "sold_count",    "sold_count_q"],
            ["discuss_count", "discuss_days",
             "onsale_count",  "onsale_days",
             "sold_count",    "sold_count_q"],
            ret_datas )
        
        return ret_datas


    def calc_sales_count_by_city_price_sub(self,
                                           ret_datas_tmp,
                                           calc_key,
                                           calc_date_from,
                                           calc_date_to):
        suumo_service = SuumoService()
        org_bukkens = suumo_service.get_bukkens_by_check_date(
            self.build_type(),
            calc_date_from,
            calc_date_to )
        
        for org_bukken in org_bukkens:
            
            for pkey in ["pref","city"]:
                if not org_bukken[pkey]:
                    org_bukken[pkey] = "?"

            # 200万円単位で丸め
            org_bukken["price"] = round(org_bukken["price"]/2000000) * 2
            if not org_bukken["price"]:
                org_bukken["price"] = "0"

            pref_city_price = "%s\t%s\t%s" % (org_bukken["pref"],
                                              org_bukken["city"],
                                              org_bukken["price"])
            
            if not pref_city_price in ret_datas_tmp:
                ret_datas_tmp[pref_city_price] = {
                    "calc_date" : calc_date_to,
                    "onsale_count" :0,  "onsale_days" :0,
                    "discuss_count":0,  "discuss_days":0,
                    "sold_count":0 }

            ret_datas_tmp[pref_city_price][calc_key+"_count"] \
                += self.house_count(org_bukken)

            tmp_days = org_bukken["check_date"] - org_bukken["found_date"]
            ret_datas_tmp[pref_city_price][calc_key+"_days"] += tmp_days.days

        return ret_datas_tmp
