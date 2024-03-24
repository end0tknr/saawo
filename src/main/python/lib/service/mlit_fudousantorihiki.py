#!python
# -*- coding: utf-8 -*-

from functools import cmp_to_key
from jeraconv import jeraconv
from psycopg2 import extras # for bulk insert
from selenium.webdriver.common.by import By
from bs4      import BeautifulSoup
from util.db  import Db
import operator
import appbase
import copy
import csv
import datetime
import glob
import json
import operator
import os
import re
import tempfile
import unicodedata
import urllib.request
import zipfile
from service.city       import CityService
from selenium.webdriver.common.alert import Alert

download_host      = 'https://www.land.mlit.go.jp'
download_init_path = '/webland/servlet/DownloadServlet'
download_param_tmpl = '?TDK=99&SKC=99999&DLF=true&TTC-From=%s&TTC-To=%s'
download_zip_path_tmpl = '/webland/zip/All_%s_%s.zip'

download_year_quatar_min = 20211

col_filters = {"種類"                   :"shurui",
               "地域"                   :"chiiki",
               "都道府県名"             :"pref",
               "市区町村名"             :"city",
               "地区名"                 :"town",
               "最寄駅：名称"           :"station",
               "最寄駅：距離（分）"     :"from_station_min",
               "取引価格（総額）"       :"price",
               "間取り"                 :"plan",
               "面積（㎡）"             :"land_area_m2",
               "延床面積（㎡）"         :"floor_area_m2",
               "建築年"                 :"build_year",
               "建物の構造"             :"structure",
               "今後の利用目的"         :"new_usage",
               "都市計画"               :"youto_chiiki",
               "取引時点"               :"trade_year_q"}

re_compile_city         = re.compile("^.+郡(.+町)$")
re_compile_num          = re.compile("^(\d+)")
re_compile_trade_year_q = re.compile("^(\d+)年第(\d)四半期")

logger = appbase.AppBase().get_logger()

class MlitFudousanTorihikiService(appbase.AppBase):

    def __init__(self):
        pass
        
    def download_save_master(self):
        download_url_pairs = self.find_download_urls()
        util_db = Db()

        for url_pair in download_url_pairs:
            self.make_download_zip( url_pair[0] )
            csv_infos = self.download_master( url_pair[1] )

            for csv_info in csv_infos:
                # bulk insert
                util_db.save_tbl_rows(
                    "mlit_fudousantorihiki",
                    ["trade_year_q","shurui","chiiki","pref","city","town",
                     "station","from_station_min","price","plan",
                     "floor_area_m2","land_area_m2","build_year",
                     "structure","new_usage","youto_chiiki"],
                    csv_info[1] )

    # https://www.land.mlit.go.jp/webland/download.html で
    # 公開済(= ダウンロード可能)なデータと
    # 既にDB保存されているデータを比較し
    # 未保存なデータのみ、ダウンロード & 保存します
    def find_download_urls(self):
        downloadable_year_quatars = self.find_download_year_quatars()
        saved_year_quatars = self.get_saved_year_quatars()

        ret_datas = []
        
        for year_quatar in downloadable_year_quatars:
            if year_quatar in saved_year_quatars:
                continue

            download_param = download_param_tmpl % (year_quatar,year_quatar)
            url_1 = download_host + download_init_path + download_param
            download_zip_path = download_zip_path_tmpl % (year_quatar,
                                                          year_quatar)
            url_2 = download_host + download_zip_path
            ret_datas.append( [url_1,url_2] )
            
        return ret_datas
    

    def get_saved_year_quatars(self,limit_year_q=0):
        sql = """
SELECT
  trade_year_q
FROM mlit_fudousantorihiki
WHERE trade_year_q >= %s
GROUP BY trade_year_q
ORDER BY trade_year_q
"""
        ret_datas = []
        
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute( sql,(limit_year_q,) )
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return []
            for ret_row in  db_cur.fetchall():
                ret_datas.append( dict( ret_row )["trade_year_q"])
        return ret_datas
    

    def get_city_quarters(self, build_type, year_quatars ):
        sql = """
SELECT pref, city, %s as summary
FROM mlit_fudousantorihiki_by_city
ORDER BY pref,city
"""
        sql = sql % (build_type+"_quarter",)
        ret_datas = []
        ret_datas_tmp = []
        
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute( sql )
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return []

            for ret_row in db_cur.fetchall():
                ret_row = dict( ret_row )
                tmp_summary = []
                if ret_row["summary"]:
                    tmp_summary = json.loads( ret_row["summary"] )
                    
                tmp_summaries = self.sort_select_quarters(tmp_summary,
                                                          year_quatars[0],
                                                          year_quatars[-1],
                                                          1)
                if len(tmp_summaries):
                    ret_datas.append({
                        "pref" : ret_row["pref"],
                        "city" : ret_row["city"],
                        "sold_count"   : tmp_summaries[0]["sold_count"],
                        "quarter_count": tmp_summaries[0]["quarter_count"],
                        "sold_price"   : tmp_summaries[0]["sold_price"] })
                else:
                    ret_datas.append({
                        "pref" : ret_row["pref"],
                        "city" : ret_row["city"],
                        "sold_count"   : 0,
                        "quarter_count": 0,
                        "sold_price"   : 0 })
                    
        return ret_datas
    
    def get_city_years(self, build_type, years ):
        sql = """
SELECT pref, city, %s as summary
FROM mlit_fudousantorihiki_by_city
ORDER BY pref,city
"""
        sql = sql % (build_type+"_year",)
        ret_datas = []
        ret_datas_tmp = []
        
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute( sql )
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return []

            for ret_row in db_cur.fetchall():
                ret_row = dict( ret_row )
                tmp_summary = []
                if ret_row["summary"]:
                    tmp_summary = json.loads( ret_row["summary"] )
                    
                ret_data = {"pref":ret_row["pref"], "city":ret_row["city"] }
                tmp_summaries = self.sort_select_years(tmp_summary,
                                                       years[0],
                                                       years[-1],
                                                       2)
                for tmp_summary_2 in tmp_summaries:
                    count_key = "sold_count_" + str(tmp_summary_2["year"])
                    price_key = "sold_price_" + str(tmp_summary_2["year"])
                    ret_data[count_key] = tmp_summary_2["sold_count"]
                    ret_data[price_key] = tmp_summary_2["sold_price"]

                ret_datas.append( ret_data )
                    
        return ret_datas
    

    def get_town_quarters(self, build_type, year_quatars ):
        sql = """
SELECT pref, city, town, %s as summary
FROM mlit_fudousantorihiki_by_town
ORDER BY pref,city,town
"""
        sql = sql % (build_type+"_quarter",)
        ret_datas = []
        ret_datas_tmp = []
        
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute( sql )
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return []

            for ret_row in db_cur.fetchall():
                ret_row = dict( ret_row )
                tmp_summary = []
                if ret_row["summary"]:
                    tmp_summary = json.loads( ret_row["summary"] )
                    
                tmp_summaries = self.sort_select_quarters(tmp_summary,
                                                          year_quatars[0],
                                                          year_quatars[-1],
                                                          1)
                if len(tmp_summaries):
                    ret_datas.append({
                        "pref" : ret_row["pref"],
                        "city" : ret_row["city"],
                        "town" : ret_row["town"],
                        "sold_count"   : tmp_summaries[0]["sold_count"],
                        "quarter_count": tmp_summaries[0]["quarter_count"],
                        "sold_price"   : tmp_summaries[0]["sold_price"] })
                else:
                    ret_datas.append({
                        "pref" : ret_row["pref"],
                        "city" : ret_row["city"],
                        "town" : ret_row["town"],
                        "sold_count"   : 0,
                        "quarter_count": 0,
                        "sold_price"   : 0 })
                    
        return ret_datas
    
    def get_town_years(self, build_type, years ):
        sql = """
SELECT pref, city, town, %s as summary
FROM mlit_fudousantorihiki_by_town
ORDER BY pref,city,town
"""
        sql = sql % (build_type+"_year",)
        ret_datas = []
        ret_datas_tmp = []
        
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute( sql )
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return []

            for ret_row in db_cur.fetchall():
                ret_row = dict( ret_row )
                tmp_summary = []
                if ret_row["summary"]:
                    tmp_summary = json.loads( ret_row["summary"] )
                    
                ret_data = { "pref":ret_row["pref"],
                             "city":ret_row["city"],
                             "town":ret_row["town"] }
                tmp_summaries = self.sort_select_years(tmp_summary,
                                                       years[0],
                                                       years[-1],
                                                       2)
                for tmp_summary_2 in tmp_summaries:
                    count_key = "sold_count_" + str(tmp_summary_2["year"])
                    price_key = "sold_price_" + str(tmp_summary_2["year"])
                    ret_data[count_key] = tmp_summary_2["sold_count"]
                    ret_data[price_key] = tmp_summary_2["sold_price"]

                ret_datas.append( ret_data )
        return ret_datas
    

    def sort_select_years(self,summaries,cmp_key_min,cmp_key_max,limit):
        ret_datas = []
        cmp_key = "year"

        for summary in sorted(summaries,
                              key=operator.itemgetter(cmp_key),
                              reverse=True):  # 直近のdataから探索

            if summary[cmp_key]<cmp_key_min or cmp_key_max<summary[cmp_key]:
                continue

            ret_datas.append(summary)
            if len(ret_datas) >= limit:
                break
        return ret_datas
    
    def sort_select_quarters(self,summaries,cmp_key_min,cmp_key_max,limit):
        ret_datas = []
        cmp_key = "year_quarter"

        for summary in sorted(summaries,
                              key=operator.itemgetter(cmp_key),
                              reverse=True):  # 直近のdataから探索

            if summary[cmp_key]<cmp_key_min or cmp_key_max<summary[cmp_key]:
                continue

            ret_datas.append(summary)
            if len(ret_datas) >= limit:
                break
        return ret_datas
    

    def get_town_summaries(self,atri_key_header, year_quatars ):
        sql = """
SELECT *
FROM mlit_fudousantorihiki_by_town
ORDER BY pref,city,town
"""
        ret_datas = []
        
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute( sql )
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return []
            for ret_row in  db_cur.fetchall():
                ret_row = dict( ret_row )
                tmp_summary = []
                if ret_row["summary"]:
                    tmp_summary = json.loads( ret_row["summary"] )

                tmp_summaries = self.sort_select_summary(tmp_summary,
                                                         year_quatars[0],
                                                         year_quatars[-1],
                                                         1)
                count_key = atri_key_header+"_sold_count"
                price_key = atri_key_header+"_sold_price"
                if len(tmp_summaries):
                    ret_datas.append({
                        "pref" : ret_row["pref"],
                        "city" : ret_row["city"],
                        "town" : ret_row["town"],
                        "sold_count" : tmp_summaries[0][count_key],
                        "sold_price" : tmp_summaries[0][price_key] })
                else:
                    ret_datas.append({
                        "pref" : ret_row["pref"],
                        "city" : ret_row["city"],
                        "town" : ret_row["town"],
                        "sold_count" : 0,
                        "sold_price" : 0 })
        return ret_datas

        
    def get_city_price_summaries(self,atri_key_header, year_quatars ):
        sql = """
SELECT pref, city, %s as summary
FROM mlit_fudousantorihiki_by_city
ORDER BY pref,city
"""
        sql = sql % (atri_key_header+"_price",)

        ret_datas = []
        re_compile = re.compile("m_yen_(\d+)")
        
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute( sql )
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return []

            for ret_row in  db_cur.fetchall():
                ret_row = dict( ret_row )
                tmp_summary = json.loads( ret_row["summary"] )
                
                for atri_key in tmp_summary.keys():
                    re_result = re_compile.search( atri_key )
                    if not re_result:
                        continue

                    price_m = re_result.group(1)
                    sold_count = tmp_summary[atri_key]
                        
                    ret_datas.append({
                        "pref" : ret_row["pref"],
                        "city" : ret_row["city"],
                        "price": price_m,
                        "sold_count" : sold_count })
        return ret_datas
    
    
    def find_download_year_quatars(self):
        # BeautifulSoup の方が安定していますが、urllib.request で
        # なぜか 「[Errno 104] Connection reset by peer」エラーとなる為、
        # selenium を使用しています
        browser = self.get_browser()
        req_url = download_host + download_init_path
        browser.get(req_url)
        
        opt_elms  = browser.find_elements(By.CSS_SELECTOR,"#TDIDTo option")
        year_quaters = []
        for opt_elm in opt_elms:
            year_quater = int( opt_elm.get_attribute("value") )
            if year_quater < download_year_quatar_min:
                continue
            year_quaters.append( year_quater )
            
        browser.close()
        year_quaters.sort()
        
        return year_quaters
        
    def make_download_zip(self, download_url):
        logger.info( download_url )

        browser = self.get_browser()
        browser.get( download_url )
        download_btns = browser.find_elements(By.CSS_SELECTOR,"#download_button")
        if len( download_btns ) == 0:
            logger.error("not found download button "+download_url )
            return False

        # 以下の操作でサーバ側で zip が作成されます
        download_btns[0].click()
        Alert( browser ).accept()
        return True
    
    def download_master(self, download_url):
        logger.info( download_url )
        
        ret_data = []
    
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_zip_path =os.path.join(tmp_dir, "tmp.zip")

            data = self.get_http_requests( download_url )

            try:
                with open(tmp_zip_path, mode="wb") as fh:
                    fh.write(data)

                zip = zipfile.ZipFile(tmp_zip_path, "r")
                zip.extractall(path=tmp_dir)
                zip.close()

            except Exception as e:
                logger.error(e)
                logger.error(download_url)
                return []

            for csv_path in glob.glob(tmp_dir + '/*.csv' ):
                csv_name = str( os.path.split(csv_path)[1] )

                # csv.DictReader() での総行数の算出方法が不明でしたので。
                dict_row_size = len(open(csv_path,encoding='cp932').readlines() )

                with open(csv_path, encoding='cp932', newline="") as f:
                    # key-value形式での読込み
                    dict_reader = csv.DictReader(f, delimiter=",", quotechar='"')
                    new_rows = []
                    i = 0
                    for dict_row in dict_reader:
                        i += 1
                        if i % 10000 == 0:
                            logger.info( "%d/%d %s" % (i,dict_row_size,csv_path))
                            
                        new_row = {}
                        # tuple -> hashmap
                        for k, v in dict_row.items():
                            new_row[k] = v
                        # 行 & 列を選定
                        new_row = self.__filter_data(new_row)
                        if new_row:
                            new_rows.append( new_row )
                        
                    ret_data.append([csv_name, new_rows])

        return ret_data

    def __filter_data(self,org_row):
        
        ret_row = {}
        for org_key,new_key in col_filters.items():
            ret_row[new_key] = org_row[org_key]
            if len(ret_row[new_key]) == 0:
                ret_row[new_key] = None

        # 〇〇郡〇〇町 -> 〇〇町
        re_result = re_compile_city.search(ret_row["city"])
        if re_result:
            ret_row["city"] = re_result.group(1)
        
        # 全角->半角
        if ret_row["plan"]:
            ret_row["plan"] = unicodedata.normalize("NFKC", ret_row["plan"])
            
        # 例 2000㎡以上->2000
        for atri_key in ["land_area_m2","floor_area_m2","from_station_min"]:
            if not ret_row[atri_key]:
                continue
            re_result = re_compile_num.search(ret_row[atri_key])
            if re_result:
                ret_row[atri_key] = int( re_result.group(1) )
                
        # 例 2014年第４四半期->20144
        for atri_key in ["trade_year_q"]:
            if not ret_row[atri_key]:
                continue
            re_result = re_compile_trade_year_q.search( ret_row[atri_key] )
            if not re_result:
                continue
            
            ret_row[atri_key] = "%s%s" % (re_result.group(1),re_result.group(2))
            ret_row[atri_key] = int(ret_row[atri_key])
            
        if ret_row["build_year"]:
            if ret_row["build_year"] == "戦前":
                ret_row["build_year"] = None
            else:
                # 和暦 -> 西暦
                try:
                    ret_row["build_year"] = \
                        jeraconv.J2W().convert(ret_row["build_year"])
                except Exception as e:
                    logger.error(e)
                    logger.error( ret_row["build_year"] )


        for atri_key in ["from_station_min","price"]:
            
            if type( ret_row[atri_key] ) is str:
            
                if len(ret_row[atri_key])==0:
                    ret_row[atri_key] = None
                else:
                    ret_row[atri_key] = float(ret_row[atri_key])

        return ret_row
    

    def calc_save_summary( self ):
        self.calc_save_city_summary()
        self.calc_save_town_summary()
    
        
    def calc_save_city_summary( self ):
        util_db = Db()
        # 市区町村単位
        city_summuries_hash = self.calc_summary( ["pref","city"] )
        city_summuries     = self.conv_summary_to_list( city_summuries_hash )

        util_db.bulk_upsert(
            "mlit_fudousantorihiki_by_city",
            ["pref","city"],
            ["pref","city",
             "newbuild_quarter","newbuild_year","newbuild_price",
             "sumstock_quarter","sumstock_year","sumstock_price"],
            ["newbuild_quarter","newbuild_year","newbuild_price",
             "sumstock_quarter","sumstock_year","sumstock_price"],
            city_summuries )
        

    def calc_save_town_summary( self ):
        util_db = Db()
        # 地区単位
        town_summuries_hash = self.calc_summary( ["pref","city","town"] )
        town_summuries     = self.conv_summary_to_list( town_summuries_hash )

        util_db.bulk_upsert(
            "mlit_fudousantorihiki_by_town",
            ["pref","city","town"],
            ["pref","city","town",
             "newbuild_quarter","newbuild_year","newbuild_price",
             "sumstock_quarter","sumstock_year","sumstock_price"],
            ["newbuild_quarter","newbuild_year","newbuild_price",
             "sumstock_quarter","sumstock_year","sumstock_price"],
            town_summuries )


    def calc_summary( self, pkeys ):
        year = datetime.datetime.today().date().year - 3
        limit_year_q = int( "%s1" % (year,))
        
        sql = """
SELECT *
FROM mlit_fudousantorihiki
WHERE shurui='宅地(土地と建物)' AND new_usage='住宅' AND
      trade_year_q >= %s
ORDER BY pref,city,trade_year_q
"""
        
        ret_datas_tmp       = {}
        re_compile = re.compile("^(.+)\d")

        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute( sql,(limit_year_q,) )
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return []

            for ret_row in  db_cur.fetchall():
                ret_row = dict( ret_row )

                town = ret_row["town"]
                if "town" in pkeys:
                    if not town:
                        continue
                    #`ex. 西町４丁目 -> 西町
                    re_result = re_compile.search(town)
                    if re_result:
                        town = re_result.group(1)
                        
                pkeys_str = ret_row["pref"] +"\t"+ ret_row["city"]
                if "town" in pkeys:
                    pkeys_str += ( "\t" + town )

                ret_datas_tmp = self.calc_summary_sub(pkeys_str,
                                                      ret_datas_tmp,
                                                      ret_row )
        return ret_datas_tmp
    

    def calc_summary_sub( self, pkeys_str,ret_datas_tmp,ret_row ):
        
        count_keys = ["newbuild_quarter","newbuild_year",
                      "sumstock_quarter","sumstock_year", ]
        price_keys = ["newbuild_price",  "sumstock_price" ]
                
        if not pkeys_str in ret_datas_tmp:
            ret_datas_tmp[pkeys_str] = {}
            for summary_key in count_keys + price_keys:
                ret_datas_tmp[pkeys_str][summary_key] = {}
            
        trade_year_q = ret_row["trade_year_q"]
        trade_year   = int( trade_year_q / 10 )

        for summary_key in count_keys:
            if not trade_year_q in ret_datas_tmp[pkeys_str][summary_key]:
                ret_datas_tmp[pkeys_str][summary_key][trade_year_q] = {
                    "sold_count":0,"sold_price":0 }
        
            if not trade_year in ret_datas_tmp[pkeys_str][summary_key]:
                ret_datas_tmp[pkeys_str][summary_key][trade_year] = {
                    "sold_count":0,"sold_price":0 }
        
        build_type  = self.newbuild_or_sumstock( ret_row )
        build_type_q = build_type + "_quarter"
        build_type_y = build_type + "_year"
        price_type   = build_type + "_price"
        
        ret_datas_tmp[pkeys_str][build_type_q][trade_year_q]["sold_count"] += 1
        ret_datas_tmp[pkeys_str][build_type_q][trade_year_q]["sold_price"] \
            += ret_row["price"]
        ret_datas_tmp[pkeys_str][build_type_y][trade_year]["sold_count"] += 1
        ret_datas_tmp[pkeys_str][build_type_y][trade_year]["sold_price"] \
            += ret_row["price"]

        # 価格帯別集計
        m_yen = self.round_200m( ret_row["price"] ) # 200万円で丸め
        m_yen_key = "m_yen_%s" % (m_yen,)
        
        if not m_yen_key in ret_datas_tmp[pkeys_str][price_type]:
            ret_datas_tmp[pkeys_str][price_type][m_yen_key] = 0
        ret_datas_tmp[pkeys_str][price_type][m_yen_key] += 1

        return ret_datas_tmp
    

    # 200万円単位で丸め
    def round_200m(self, price):
        price_m = round( price /2000000 ) * 2
        return price_m
    
    
    def conv_summary_to_list(self, ret_datas_tmp ):
    
        quarter_keys = ["newbuild_quarter","sumstock_quarter"]
        year_keys    = ["newbuild_year",   "sumstock_year"   ]
        price_keys   = ["newbuild_price",  "sumstock_price"  ]

        ret_datas = []
        for pref_city, ret_datas_tmp_2 in ret_datas_tmp.items():
            pkeys = pref_city.split("\t")

            summaries = {}
            for summary_key in quarter_keys+year_keys:
                summaries[summary_key] = []
            for summary_key in price_keys:
                summaries[summary_key] = {}

            for summary_key in quarter_keys:
                ret_datas_tmp_3 = ret_datas_tmp_2[summary_key]
                for year_quarter, summary in ret_datas_tmp_3.items():
                    summary["year_quarter"] = year_quarter

                    if not summary["sold_count"]:
                        continue
                        
                    #平均価格
                    summary["sold_price"] = \
                        summary["sold_price"] / summary["sold_count"]
                    summary["sold_price"] = round( summary["sold_price"] )
                    
                    # 週次の値の表示用に、計12で除算
                    summary["quarter_count"] = summary["sold_count"]
                    summary["sold_count"] = round(summary["sold_count"] / 12, 2)
                    
                    summaries[summary_key].append(summary)
                    
            for summary_key in year_keys:
                ret_datas_tmp_3 = ret_datas_tmp_2[summary_key]
                for year, summary in ret_datas_tmp_3.items():
                    summary["year"] = year
                    
                    if not summary["sold_count"]:
                        continue
                        
                    #平均価格
                    summary["sold_price"] = \
                        summary["sold_price"] / summary["sold_count"]
                    summary["sold_price"] = round( summary["sold_price"] )

                    summaries[summary_key].append(summary)
                    
            for summary_key in price_keys:
                summaries[summary_key] = ret_datas_tmp_2[summary_key]
                    
            ret_data = {}
            if len(pkeys) == 2:
                ret_data = {"pref":pkeys[0],"city":pkeys[1]}
            else:
                ret_data = {"pref":pkeys[0],"city":pkeys[1],"town":pkeys[2] }

            for summary_key in quarter_keys+year_keys+price_keys:
                ret_data[summary_key]= json.dumps( summaries[summary_key],
                                                   ensure_ascii=False )
            ret_datas.append( ret_data )
        return ret_datas

    # 取引時期と建築時期の差で、新築or中古を判定
    def newbuild_or_sumstock( self, ret_row ):
        if not ret_row["build_year"]:
            return "sumstock"
        
        year_diff = int(ret_row["trade_year_q"]/10) - int(ret_row["build_year"])
        if year_diff <= 2:
            return "newbuild"
        return "sumstock"
