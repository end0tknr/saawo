#!python
# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from service.city                 import CityService
from service.mlit_realestateshop  import MlitRealEstateShopService
from util.db import Db

import appbase
import concurrent.futures # 並列処理用
import datetime
import json
import re
import sys
import time
import unicodedata   # 全角→半角変換や、normalize用. 標準module
import urllib.parse
import urllib.request

pref_names = [
    # "hokkaido","aomori","iwate","miyagi","akita","yamagata",
    # "fukushima","tochigi",
    # "gumma",            # suumo では、gunma でなく gumma
    "ibaraki","saitama","chiba",
    "tokyo",
    "kanagawa",
    # "niigata","toyama","ishikawa","fukui","yamanashi","nagano","gifu",
    # "shizuoka","aichi","mie","shiga","kyoto","osaka","hyogo","nara",
    # "wakayama","tottori","shimane","okayama","hiroshima","yamaguchi",
    # "tokushima","kagawa","ehime","kochi",
    # "fukuoka","saga","nagasaki",
    # "kumamoto","oita","miyazaki", "kagoshima"
]

base_host = "https://suumo.jp"

base_urls = [
    #[base_host+"/ikkodate/",       "新築戸建"],
    [base_host+"/chukoikkodate/",  "中古戸建"],
    # 新築マンションは価格等が記載されていないことが多い為、無視
    #[base_host+"/ms/shinchiku/",  "新築マンション"]
    # 上記の通り、新築マンションを対象外とするなら
    # 中古マンションを取得する意味がないので、無視
    #[base_host+"/ms/chuko/",       "中古マンション"],
]

# 宅建業者の免許番号等から、不動産会社を抽出
re_compile_licenses = [
    re.compile("会社概要.+?((国土交通大臣).{0,6}第(\d\d+)号)"),
    # refer to https://techacademy.jp/magazine/20932
    re.compile("会社概要.+?((神奈川県|和歌山県|鹿児島県|[一-龥]{2}[都道府県])"+
               ".{0,10}知事.{0,6}第(\d\d+)号)") ]

re_compile_house_count_1 = re.compile(
    "販売.{0,2}?数\s*ヒント\s*(\d+)\s*(戸|室|棟|区画)")
re_compile_house_count_2 = re.compile(
    "総.{0,2}?数\s*ヒント\s*(\d+)\s*(戸|室|棟|区画)")

re_compile_show_date = re.compile("情報提供日.{0,10}(20\d+)年(\d+)月(\d+)日")

parallel_size = 4  # 並列処理用

check_date_diff = -1
logger = appbase.AppBase().get_logger()

class SuumoService(appbase.AppBase):
    
    def __init__(self):
        pass

    def save_bukken_infos_main(self):
        func_name = sys._getframe().f_code.co_name
        logger.info("start %s",func_name)

        # 物件一覧のurl探索
        result_list_urls = self.find_search_result_list_url()
        # 物件一覧の旧url 削除
        self.del_search_result_list_urls()

        # 物件一覧の新url 登録
        for build_type, result_list_urls in result_list_urls.items():
            self.save_search_result_list_urls(build_type,result_list_urls)
            
        # 物件一覧の新url 再? load
        result_list_urls = self.load_search_result_list_urls()
            
        # 各物件情報の取得と保存
        i = 0
        for result_list_tmp in result_list_urls:
            i += 1
            build_type      = result_list_tmp[0]
            result_list_url = result_list_tmp[1]

            if i % 20 == 0:
                logger.info("%d/%d %s %s" % (i,len(result_list_urls),
                                             build_type,
                                             result_list_url ))
            
            bukken_infos = self.parse_bukken_infos(result_list_url)
            bukken_infos = self.conv_bukken_infos_for_upsert(build_type,
                                                             bukken_infos )
            util_db = Db()
            util_db.bulk_upsert(
                "suumo_bukken",
                ["url"],
                ["url","build_type","bukken_name","price","price_org",
                 "pref","city","address","plan","build_area_m2",
                 "build_area_org","land_area_m2","land_area_org",
                 "build_year","shop_org",
                 "found_date","check_date","update_time"],
                ["build_type","bukken_name","price","price_org",
                 "pref","city","address","plan","build_area_m2",
                 "build_area_org","land_area_m2","land_area_org",
                 "build_year","shop_org",
                 "check_date","update_time"],
                bukken_infos )


    def save_bukken_details(self,build_type,other_where):
        class_name = self.__class__.__name__
        func_name  = sys._getframe().f_code.co_name
        logger.info("start %s %s %s %s",class_name,func_name,build_type,other_where)
        
        org_bukkens = self.get_bukkens_for_detail(build_type,other_where)
        org_size = len(org_bukkens)

        new_bukkens = []
        bulk_insert_size = self.get_conf()["common"]["bulk_insert_size"]
        
        while len(org_bukkens) >= parallel_size:
            parallels = []
            i = 0
            while i < parallel_size:

                calced = org_size - len(org_bukkens)
                if calced % 100 == 0:
                    logger.info("{} {}/{}".format(build_type,calced,org_size) )

                # refer to https://pystyle.info/python-concurrent-futures/
                with concurrent.futures.ThreadPoolExecutor() as executor:
                # with concurrent.futures.ProcessPoolExecutor() as executor:
                    future = executor.submit(self.parse_bukken_detail,
                                             org_bukkens.pop() )
                    parallels.append(future)
                    i += 1

            datetime_now = datetime.datetime.now()
            
            for parallel in parallels:
                bukken_detail = parallel.result()
                if not bukken_detail:
                    continue

                bukken_detail["update_time"] = datetime_now
                new_bukkens.append( bukken_detail )

            if len(new_bukkens) >= bulk_insert_size:
                util_db = Db()
                util_db.bulk_update(
                    "suumo_bukken",
                    ["url"],
                    ["url","shop","total_house","house_for_sale",
                     "show_date","update_time"],
                    new_bukkens )
                new_bukkens = []
                

        if len(org_bukkens):
            parallels = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(self.parse_bukken_detail,
                                         org_bukkens.pop() )
                parallels.append(future)
            i += 1

            datetime_now = datetime.datetime.now()
        
            new_bukkens = []
            for parallel in parallels:
                bukken_detail = parallel.result()
                if not bukken_detail:
                    continue

                bukken_detail["update_time"] = datetime_now
                new_bukkens.append( bukken_detail )
            
            if len(new_bukkens):
                util_db = Db()
                util_db.bulk_update(
                    "suumo_bukken",
                    ["url"],
                    ["url","shop","total_house","house_for_sale",
                     "show_date","update_time"],
                    new_bukkens )
            
            
    def modify_pref_city(self,address_org,pref,city,other):
        sql = """
UPDATE suumo_bukken
SET pref=%s, city=%s, address=%s
WHERE address=%s
"""
        sql_args = (pref,city,other,address_org)
        
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute(sql,sql_args)
                db_conn.commit()
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return False

        return True
        
    def load_all_bukkens(self):
        ret_rows = []
        sql = """
SELECT * FROM suumo_bukken where pref =''
"""
        
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute(sql)
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return []

            ret_rows = db_cur.fetchall()
                
        for ret_row in ret_rows:
            ret_row = dict( ret_row )
            
        return ret_rows
        
    def load_search_result_list_urls(self):
        logger.info("start")

        sql = "SELECT * FROM suumo_search_result_url"
        ret_rows = []

        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute(sql)
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return []
            
            for row in db_cur.fetchall():
                ret_rows.append( [row["build_type"],row["url"]] )
                
        return ret_rows
    
    def conv_bukken_infos_for_upsert(self, build_type, bukken_infos ):
        datetime_now = datetime.datetime.now()
            
        for bukken_info in bukken_infos:
            bukken_info["build_type"] = build_type
            
            for atri_key in ['price','build_year']:
                if not bukken_info[atri_key]:
                    bukken_info[atri_key] = "0"
                bukken_info[atri_key] = int( bukken_info[atri_key] )
                    
                for atri_key in ['build_area_m2','land_area_m2']:
                    if not bukken_info[atri_key]:
                        bukken_info[atri_key] = "0"
                    bukken_info[atri_key] = float( bukken_info[atri_key] )
                        
                bukken_info["found_date"] = datetime_now
                bukken_info["check_date"] = datetime_now
                bukken_info["update_time"]= datetime_now
        return bukken_infos
    
    def del_search_result_list_urls(self):
        logger.info("start")

        sql = "delete from suumo_search_result_url"

        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute(sql)
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return False
        db_conn.commit()
        return True
        
    def save_search_result_list_urls(self, build_type, urls):
        logger.info("start "+ build_type)

        save_rows = []
        for url in urls:
            save_rows.append({"build_type":build_type,"url":url})

        util_db = Db()
        util_db.save_tbl_rows("suumo_search_result_url",
                              ["build_type","url"],
                              save_rows )


    def divide_rows_list(self, build_type, org_rows, chunk_size):
        i = 0
        chunk = []
        ret_rows = []
        for org_row in org_rows:
            chunk.append( (build_type, org_row) )
            
            if len(chunk) >= chunk_size:
                ret_rows.append(chunk)
                chunk = []
            i += 1

        if len(chunk) > 0:
            ret_rows.append(chunk)

        return ret_rows

        
    def find_search_result_list_url(self):
        logger.info("start")

        ret_urls = {}
        for base_url_tmp in base_urls:
            base_url   = base_url_tmp[0]
            build_type = base_url_tmp[1]
            for pref_name in pref_names:
                #他の都道府県のurl構成が異なる為、無視(skip)
                if pref_name == "hokkaido" and \
                     base_url == "https://suumo.jp/ms/shinchiku/":
                    continue
                
                #「hokkaido_」のように「_」が付加されている為
                if pref_name == "hokkaido" and \
                   base_url in ("https://suumo.jp/ikkodate/",
                                "https://suumo.jp/chukoikkodate/",
                                "https://suumo.jp/ms/chuko/"):
                    pref_name += "_"
                
                tmp_urls = self.find_search_result_list_url_sub(base_url,
                                                                pref_name)
                if not build_type in ret_urls:
                    ret_urls[build_type] = []
                
                ret_urls[build_type].extend(tmp_urls)
                
        return ret_urls
    

    def find_search_result_list_url_sub(self, base_url, pref_name):
        logger.info("%s %s" % (base_url, pref_name))

        browser = self.get_browser()
        
        req_url = base_url + pref_name +"/city/"
        browser.get( req_url )
        
        # 検索ボタン click
        css_selector = ".ui-btn--search"
        submit_btns = browser.find_elements(By.CSS_SELECTOR,css_selector)

        if len(submit_btns) == 0:
            logger.error(req_url +" "+css_selector)
            browser.close()
            return []

        submit_btns[0].click()
        time.sleep(3)

        paginations = []
        paginations.extend(
            browser.find_elements(By.CSS_SELECTOR,
                                 ".pagination.pagination_set-nav ol li"))
        paginations.extend(
            browser.find_elements(By.CSS_SELECTOR,
                                 ".sortbox_pagination ol li") )

        ret_urls = [browser.current_url]
        if len(paginations) == 0:
            return ret_urls

        for pno in range( 1, int(paginations[-1].text) ):
            ret_urls.append("%s&pn=%d" % (browser.current_url, pno+1) )

        browser.close()
        return ret_urls
    
    
    def parse_bukken_infos(self, result_list_url):

        html_content = self.get_http_requests( result_list_url )
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, 'html.parser')
        
        ret_bukken_infos = []
        bukken_parent_divs = soup.select("div.property_unit-content")
        # bukken_divs = soup.select("div.dottable.dottable--cassette")
        
        for bukken_div in bukken_parent_divs:
            bukken_info = {}
            
            result_parse = self.parse_bukken_url(bukken_div)
            if not len(result_parse):
                logger.error("fail parse url "+ result_list_url)
                logger.error(bukken_div)
                continue

            bukken_info["url"]    = result_parse[0]
            bukken_info["物件名"] = result_parse[1]
                
            
            dls = bukken_div.select("dl")
            for dl in dls:
                dts = dl.select("dt")
                dds = dl.select("dd")
                if len(dts) == 0 or len(dds) == 0:
                    continue

                atri_name = dts[0].text.strip()
                
                if atri_name != "物件名" or \
                   (atri_name == "物件名" and not bukken_info["物件名"]):
                    
                    bukken_info[ atri_name ] = dds[0].text.strip()

            bukken_info["shop_org"] = self.find_shop_name( bukken_div,
                                                           bukken_info["url"] )
            ret_bukken_infos.append( self.conv_bukken_info(bukken_info) )
        return ret_bukken_infos
    
    
    def parse_bukken_url(self, bukken_div):
        a_elms = bukken_div.select(".property_unit-title a")
        if not len(a_elms):
            return []

        re_compile = re.compile("href=[\"\']?([^\s\"\']+)[\"\']?")
        re_result = re_compile.search( str(a_elms[0]) )
        if not re_result:
            return []

        bukken_detail_url = base_host+re_result.group(1)
        bukken_name = a_elms[0].text.strip()
        return [bukken_detail_url,bukken_name]

        
    def find_shop_name(self, bukken_div, bukken_url ):

        divs = bukken_div.select("div.shopmore-title")
        if not len(divs):
            return None
        
        shop_org = self.parse_shop_name( divs[0].text.strip() )
        if shop_org:
            return shop_org

        return None

    def parse_shop_name(self, org_shop_name):
        kabu_re_exp = "(?:株式会社|有限会社|\(株\)|\（株\）|\(有\)|\（有\）)"
        shop_re_exp  = "([^\(\)（）]+)"
        
        if not org_shop_name:
            return None
        # 後株
        re_compile = re.compile( "^"+ shop_re_exp + kabu_re_exp )
        re_result = re_compile.search(org_shop_name)
        if re_result:
            return re_result.group(1)
        # 前株
        re_compile = re.compile( kabu_re_exp + shop_re_exp + "$" )
        re_result = re_compile.search(org_shop_name)
        if re_result:
            return re_result.group(1)

        return org_shop_name
    
    def conv_bukken_info(self,org_info):
        
        org_new_keys = {
            '物件名'  :'bukken_name',
            '販売価格':'price_org',
            '所在地'  :'address',
            '間取り'  :'plan',
            '土地面積':'land_area_org',
            '土地面積':'land_area_org',
            '築年月'  :'build_year',
            'url'     :'url',
            'shop_org':'shop_org',
        }
        ret_info = {}
        for org_key,new_key in org_new_keys.items():
            if not org_key in org_info:
                ret_info[new_key] = None
                continue
            ret_info[new_key] = org_info[org_key] or None

        address_org = ret_info["address"]
        city_service  = CityService()
        address_new = city_service.parse_pref_city(address_org)
        ret_info["pref"]    = address_new[0]
        ret_info["city"]    = address_new[1]
        ret_info["address"] = address_new[2]

        for org_key in ["建物面積","専有面積"]:
            if org_key in org_info:
                ret_info["build_area_org"] = org_info[org_key]
        if not "build_area_org" in ret_info:
            ret_info["build_area_org"] = None

        ret_info["build_area_m2"] = self.conv_area( ret_info["build_area_org"] )
        ret_info["land_area_m2"]  = self.conv_area( ret_info["land_area_org"] )
        ret_info["price"]         = self.conv_price( ret_info["price_org"] )
        ret_info["build_year"]    = self.conv_build_year( ret_info["build_year"] )
        
        return ret_info
    

    def conv_area(self, org_val ):
        if not org_val or org_val == "-":
            return None

        # 中央値を返す
        re_compile_val_2 = \
            re.compile("([\d\.]{2,10})(?:m2|㎡).+?([\d\.]{2,10})(?:m2|㎡)")
        re_result = re_compile_val_2.search( org_val )
        if re_result:
            ret_val = float(re_result.group(1)) + float(re_result.group(2))
            return ret_val /2
        
        re_compile_val_1 = re.compile("([\d\.]{2,10})(?:m2|㎡)")
        re_result = re_compile_val_1.search( org_val )
        if re_result:
            ret_val = float(re_result.group(1))
            return ret_val

        logger.error( org_val )
        
    def conv_price(self, org_val ):
        if not org_val:
            return None
        if org_val in ["未定"]:
            return None

        # 「???円～???円」表記の場合、中央値(万円)を返す
        re_compile_val_1 = \
            re.compile("([\d\.]{1,10})(万|億)[^\d]+?([\d\.]{1,10})(万|億)")
        re_result = re_compile_val_1.search( org_val )
        if re_result:
            ret_val = (int(re_result.group(1)) + int(re_result.group(3))) /2
            if re_result.group(2) == "万":
                ret_val *= 10000
            elif re_result.group(2) == "億":
                ret_val *= 100000000
            return ret_val

        re_compile_val_2 = re.compile("([\d\.]{1,5})億([\d\.]{1,5})万")
        re_result = re_compile_val_2.search( org_val )
        if re_result:
            ret_val =  int( re_result.group(1) ) * 100000000 # 億
            ret_val += int( re_result.group(2) ) * 10000     # 万
            return ret_val

        re_compile_val_1 = re.compile("([\d\.]{1,10})(万|億)")
        re_result = re_compile_val_1.search( org_val )
        if re_result:
            ret_val = int( re_result.group(1) )

            if re_result.group(2) == "万":
                ret_val *= 10000
            elif re_result.group(2) == "億":
                ret_val *= 100000000
            return ret_val
        
        logger.error( org_val )
        
    def conv_build_year(self, org_val ):
        if not org_val:
            return None

        re_compile_val_1 = re.compile("(\d\d\d\d)年")
        re_result = re_compile_val_1.search( org_val )
        if re_result:
            ret_val = int(re_result.group(1))
            return ret_val

        logger.error( org_val )

        
    def get_vals_group_by_city_sub(self, start_date_str, end_date_str):
        sql = """
select
  pref, city, build_type,
  count(*) as count,
  round(avg(price))::bigint as price
from suumo_bukken
where
  (check_date between %s AND %s)
group by pref,city,build_type
order by build_type, count(*) desc
"""
        ret_data_tmp = {}
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute(sql, (start_date_str, end_date_str))
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return []

            for ret_row in  db_cur.fetchall():
                ret_row= dict( ret_row )
                pref_city = "%s\t%s" % (ret_row['pref'],ret_row['city'])
                
                if not pref_city in ret_data_tmp:
                    ret_data_tmp[pref_city] = {}
                        
                build_type = ret_row['build_type']
                ret_data_tmp[pref_city][build_type+"_count"]=ret_row['count']
                ret_data_tmp[pref_city][build_type+"_price"]=ret_row['price']
                    
        ret_data = []
        for pref_city_str,key_vals in ret_data_tmp.items():
            pref_city = pref_city_str.split("\t")
            key_vals["pref"] = pref_city[0]
            key_vals["city"] = pref_city[1]
            ret_data.append(key_vals)

                
        return ret_data
    
    def get_last_check_date(self):
        sql = """
select
  check_date
from suumo_bukken
order by check_date desc
limit 1
"""
        ret_data_tmp = {}
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute(sql)
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return None

            ret_row = db_cur.fetchone()
            return ret_row[0]
        
    def get_stock_vals(self):
        check_date = self.get_last_check_date()
        check_date_str = check_date.strftime('%Y-%m-%d')

        return self.get_vals_group_by_city_sub(check_date_str,check_date_str)

    def get_sold_vals(self):
        check_date = self.get_last_check_date()
        end_date = check_date + datetime.timedelta(days= -1)
        end_date_str = end_date.strftime('%Y-%m-%d')
        start_date_str = end_date.strftime('%Y-%m-01')

        return self.get_vals_group_by_city_sub(start_date_str,end_date_str)


    
    def parse_bukken_detail(self, org_bukken):
        if not "url" in org_bukken or not org_bukken["url"]:
            logger.error( "no url in {}".format(org_bukken))
            return None

        req_url = org_bukken["url"] + "bukkengaiyo/"

        html_content = self.get_http_requests( req_url )
        if not html_content:
            logger.warning("fail http {}".format(req_url))
            return None

        ret_data = {"url": org_bukken["url"] }
        soup = BeautifulSoup(html_content, 'html.parser')
        
        shop_def = self.parse_bukken_shop(soup)
        if shop_def:
            ret_data["shop"] = shop_def["shop"]
        else:
            logger.warning("no shop_def {}".format(req_url))
            ret_data["shop"] = None

        house_count = self.parse_bukken_house_count(soup)
        ret_data.update(house_count)

        ret_data["show_date"] = self.parse_bukken_show_date(soup)

        return ret_data
    

    def parse_bukken_house_count(self, soup):
        all_text = soup.text.strip().replace("\n"," ")
        
        ret_data = {}
        re_result = re_compile_house_count_1.search(all_text)
        if re_result:
            # なぜか全角文字が紛れ込むようですので NFKC
            ret_data["house_for_sale"] = \
                unicodedata.normalize("NFKC",re_result.group(1))
            ret_data["house_for_sale"] = int( ret_data["house_for_sale"] )
        else:
            ret_data["house_for_sale"] = 0
            
        re_result = re_compile_house_count_2.search(all_text)
        if re_result:
            # なぜか全角文字が紛れ込むようですので NFKC
            ret_data["total_house"] = \
                unicodedata.normalize("NFKC",re_result.group(1))
            ret_data["total_house"] = int( ret_data["total_house"] )
        else:
            ret_data["total_house"] = 0
            
        return ret_data
    
    def parse_bukken_show_date(self, soup):
        all_text = soup.text.strip().replace("\n"," ")
        re_result = re_compile_show_date.search(all_text)
        if not re_result:
            return None

        show_date = "%s-%s-%s" % (re_result.group(1),
                                  re_result.group(2),
                                  re_result.group(3))
        ret_date_str = unicodedata.normalize("NFKC",show_date) #全角→半角
        ret_date = datetime.datetime.strptime(ret_date_str, '%Y-%m-%d')

        return ret_date
        
    def parse_bukken_shop(self, soup):
        all_text = soup.text.strip().replace("\n"," ")
        # all_text = soup.text.strip()

        shop_service = MlitRealEstateShopService()

        for re_compile in re_compile_licenses:
            re_result = re_compile.search( all_text )
            if not re_result:
                continue
                
            government = re_result.group(2)
            licence    = unicodedata.normalize("NFKC",re_result.group(3))
            licence    = "第%06d号" % (int(licence))

            shop_def = shop_service.get_def_by_licence(government,licence)
            if shop_def:
                return shop_def

            logger.info(
                "shop_service.find_licence_def() for {} {}".format(government,licence) )

            # DBにない場合、一旦、以下にて検索
            # https://etsuran.mlit.go.jp/TAKKEN/takkenKensaku.do
            shop_service.find_licence_def(licence)
            shop_def = shop_service.get_def_by_licence(government,licence)
            logger.debug( shop_def )

            return shop_def
                
        return {}
    
    
    def get_last_check_date(self):
        sql = """
SELECT check_date FROM suumo_bukken
ORDER BY check_date DESC
LIMIT 1
"""
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute(sql)
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return None

            ret_rows = db_cur.fetchall()
            ret_row = dict( ret_rows[0] )
            return str( ret_row["check_date"] )
        return None

        
    def get_bukkens_by_check_date(self, build_type, date_from, date_to):
        ret_rows = []
        
        sql = """
SELECT * FROM suumo_bukken
WHERE build_type=%s and (check_date BETWEEN %s AND %s)
      and shop is not null;
"""
        sql_args = (build_type, date_from, date_to )
        
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute(sql,sql_args)
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return []

            ret_rows = db_cur.fetchall()
                
        ret_datas = []
        for ret_row in ret_rows:
            ret_row = dict( ret_row )
            ret_datas.append( ret_row )
        return ret_rows
        
    def get_bukkens_for_detail(self, build_type,other_where):
        ret_rows = []
        sql = """
SELECT * FROM suumo_bukken
WHERE build_type=%s and check_date >= %s
"""
        if other_where:
            sql += (" AND "+ other_where)
        
        chk_date_str = self.get_last_check_date()
        chk_date = datetime.datetime.strptime(chk_date_str, '%Y-%m-%d')
        
        limit_date = chk_date + datetime.timedelta(days= check_date_diff )
        limit_date_str = limit_date.strftime('%Y-%m-%d')
        
        logger.info("limit_date:"+ limit_date_str)
        
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute(sql,
                               (build_type,
                                limit_date_str))
            except Exception as e:
                logger.error(e)
                logger.error(sql)
                return []

            ret_rows = db_cur.fetchall()
                
        ret_datas = []
        for ret_row in ret_rows:
            ret_row = dict( ret_row )
            ret_datas.append( ret_row )
        return ret_rows
        
