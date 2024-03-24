#!python
# -*- coding: utf-8 -*-

from selenium.webdriver.common.by  import By
from selenium.webdriver.support.ui import Select
from util.db import Db

import appbase
import os
import jaconv        # pip install jaconv
import re
import sys
import time
import unicodedata   # 標準module

# 国土交通省の宅地建物取引業者検索システム用
url_base = "https://etsuran2.mlit.go.jp/TAKKEN/takkenKensaku.do"
api_args_tmpl = \
    "CMD=search&sv_rdoSelect=1&sv_rdoSelectJoken=1&sv_rdoSelectSort=1&"+\
    "sv_kenCode=&sv_choice=1&sv_sortValue=1&sv_pageListNo1=1&sv_pageListNo2=1&"+\
    "sv_comNameKanaOnly=&sv_comNameKanjiOnly=&sv_licenseNoKbn=&"+\
    "sv_licenseNoFrom=%s&sv_licenseNoTo=%s&sv_licenseNo=&sv_dispCount=50&"+\
    "sv_dispPage=1&resultCount=5&pageCount=1&dispPage=1&caller=TK&rdoSelect=1&"+\
    "comNameKanaOnly=&comNameKanjiOnly=&rdoSelectJoken=1&licenseNoKbn=&"+\
    "licenseNoFrom=%s&licenseNoTo=%s&choice=1&kenCode=&sortValue=1&"+\
    "rdoSelectSort=1&dispCount=50&pageListNo1=1&pageListNo2=1"

insert_cols = ["government","licence","shop"]

logger = appbase.AppBase().get_logger()

class MlitRealEstateShopService(appbase.AppBase):

    def __init__(self):
        pass

    # 宅地建物取引業者検索システムを 免許番号で検索し
    # 不動産会社の名称をDBへ登録します。
    # なぜか、BeautifulSoup では動作しなかった為、seleniumを使用.
    # しかも、宅地建物取引業者検索システムは、
    # 元々、不安定? リソース不足? の為、動作が固まります。
    def find_licence_def(self,licence_no):

        licence_no = licence_no.replace("第","").replace("号","")
        
        api_args = api_args_tmpl % (licence_no,licence_no,licence_no,licence_no)
        req_url = url_base + "?" + api_args
        browser = self.get_browser()
        time.sleep(2)
        browser.get(req_url)
        shops = self.parse_found_shops_pages(browser,1)

        util_db = Db()
        util_db.bulk_upsert("real_estate_shop",
                            ['government','licence'],
                            ['government','licence','shop'],
                            ['shop'],
                            shops )
        return shops

    def download_and_save_master(self):
        func_name = sys._getframe().f_code.co_name
        logger.info("start %s",func_name)

        pref_no     = 40
        #pref_no     = 1
        max_pref_no = 47 # 47都道府県

        while pref_no <= max_pref_no:
            req_url = url_base + "?dispCount=50&choice=1&kenCode=%02d" %(pref_no)
            logger.info("PREF %s/%s %s",pref_no,max_pref_no,req_url)

            browser = self.get_browser()
            time.sleep(2)
            browser.get(req_url)
            time.sleep(2)
            search_btn = self.find_search_btn( browser )
            search_btn.click()
            
            # parseした不動産会社情報のdb保存
            shops = self.parse_found_shops_pages(browser)
            
            self.save_tbl_rows(shops)

            browser.close()
            pref_no += 1

    def parse_found_shops_pages(self, browser,limit_i=1000):
        class_name = self.__class__.__name__
        func_name  = sys._getframe().f_code.co_name
        shops_hash = {}
        i = 0
        while(i < limit_i ):  # 1000は 最終pageを判定できない場合に備えたもの
            body = browser.find_element(by=By.CSS_SELECTOR,value="body")
            body_text = body.text.replace("\n"," ")
            # 「接続拒否」の場合は、一旦、BACK
            if "The requested URL was rejected" in body_text:
                tmp_msg = "The requested URL was rejected... and bak"
                logger.warning(tmp_msg)
                a_elms = browser.find_elements(by=By.CSS_SELECTOR, value="a")
                a_elms[0].click() # 「Go Back」link
            
            shops_hash_tmp = self.parse_shops( browser )
            shops_hash.update( shops_hash_tmp )

            select_elms = browser.find_elements(by=By.CSS_SELECTOR,
                                                value="#pageListNo1")
            if len(select_elms) == 0:
                logger.error("fail find_elements() for {}".format(browser.current_url))
                i += 1
                continue
                
            page_no = 0
            try:
                page_no = \
                    Select(select_elms[0]).first_selected_option.text.split("/")
            except Exception as e:
                logger.error(e)
                logger.error("fail find page no... retry")
                browser.back()
                continue
                
            if i % 10 == 0:
                logger.info("%s/%s %s" %
                            (page_no[0],page_no[1],browser.current_url) )
            
            if page_no[0] == page_no[1]:  #最終pageに達したら、終了
                break

            next_btn = self.find_next_btn( browser )
            next_btn.click()
            time.sleep(3)
            i += 1

        ret_datas = []
        for pref_licence_str,shop in shops_hash.items():
            pref_licence = pref_licence_str.split("\t")
            ret_datas.append({"government": pref_licence[0],
                              "licence"   : pref_licence[1],
                              "shop"      : shop} )
        return ret_datas


    def conv_shop_name(self, shop):
        replace_strs = ["株式会社","有限会社","合資会社","合同会社",'合名会社',
                        '独立行政法人','特定非営利活動法人','社会福祉法人',
                        '一般社団法人',"一般財団法人","公益財団法人"]
        for replace_str in replace_strs:
            shop = shop.replace(replace_str,"")

        shop = shop.strip().strip("　")

        # 英数字とカナを半角化
        shop = unicodedata.normalize("NFKC", shop)
        shop = jaconv.z2h(shop, kana=True, ascii=False, digit=False)
        
        return shop

    def parse_shops( self, browser ):
        tr_elms = browser.find_elements(by=By.CSS_SELECTOR,
                                        value="table.re_disp tr")
        if len(tr_elms) == 0:
            logger.error("fail parse table.re_disp tr {}".format(browser.current_url))
            return {}
        
        tr_elms.pop(0) # 先頭行はヘッダの為、削除

        re_compile = re.compile("[\(（].+[\)）]")

        shops_tmp = {}
        for tr_elm in tr_elms:
            cols_str = tr_elm.text
            cols = tr_elm.text.split(" ")

            government = "";
            licence    = "";
            shop       = "";

            if len(cols) == 7:
                government = re_compile.sub('',cols[1])
                government = government.replace("各地方整備局等","国土交通大臣")
                
                licence    = re_compile.sub('',cols[2])
                shop = cols[3]
            elif len(cols) == 6:    #信託銀行の場合
                government = "-"
                licence    = re_compile.sub('',cols[1])
                shop       = cols[2]
            else:
                continue

            

            shop = self.conv_shop_name(shop)

            shop_key = government +"\t"+ licence
            shops_tmp[shop_key] = shop

        return shops_tmp

    def find_search_btn( self, browser ):
        img_elms = browser.find_elements(by=By.CSS_SELECTOR, value="img")
        for img_elm in img_elms:
            img_src = img_elm.get_attribute("src")
            if "btn_search_off.png" in img_src:
                return img_elm
        return None

    def find_next_btn( self, browser ):
        img_elms = browser.find_elements(by=By.CSS_SELECTOR, value="img")
        for img_elm in img_elms:
            img_src = img_elm.get_attribute("src")
            if "result_move_r.jpg" in img_src:
                return img_elm
        return None
            
    def get_def_by_licence(self,government,licence):
        class_name = self.__class__.__name__
        func_name  = sys._getframe().f_code.co_name

        sql = """
select * from real_estate_shop
where government=%s and  licence=%s
"""
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            try:
                db_cur.execute(sql,(government,licence))
            except Exception as e:
                logger.error(class_name,func_name,e,sql)
                return {}

            ret_rows = db_cur.fetchall()
            if len(ret_rows):
                return dict(ret_rows[0])

            logger.warning("Not found %s %s %s %s",
                           class_name,func_name,government,licence)
            return None
        
    def del_tbl_rows(self):
        func_name = sys._getframe().f_code.co_name
        logger.info("start %s",func_name)
        util_db = Db()
        util_db.del_tbl_rows("real_estate_shop")

    def save_tbl_rows(self, rows):
        func_name = sys._getframe().f_code.co_name
        logger.info("start %s",func_name)
        util_db = Db()
        util_db.save_tbl_rows("real_estate_shop",insert_cols,rows )
        

