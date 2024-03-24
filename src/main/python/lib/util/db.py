#!python
# -*- coding: utf-8 -*-

from psycopg2  import extras # for bulk insert
import appbase
import sys

logger = appbase.AppBase().get_logger()

class Db(appbase.AppBase):
    
    def __init__(self):
        pass

    def col_defs(self,tbl_name):
        conf = self.get_conf()
        db_conn = self.db_connect()
        cur = self.db_cursor(db_conn)
        sql = """
select
  isc.column_name, isc.data_type, pd.description
from information_schema.columns as isc
left join pg_description as pd
  on ( pd.objsubid=isc.ordinal_position )
left join pg_stat_user_tables as psut
  on (pd.objoid=psut.relid and psut.relname=isc.table_name)
where isc.table_catalog=%s and isc.table_name=%s
ORDER BY isc.ORDINAL_POSITION
"""
        try:
            cur.execute(sql, [conf["db"]["db_name"],tbl_name])
        except Exception as e:
            logger.error(e)
            return []
        
        ret_rows = []
        for row in cur.fetchall():
            ret_rows.append( dict(row) )
            
        return ret_rows

    def save_col_comment(self,tbl_name,col_name,comment):
        logger.info( " ".join([tbl_name,col_name,comment]) )

        conf = self.get_conf()
        db_conn = self.db_connect()
        cur = self.db_cursor(db_conn)
        sql = "COMMENT ON COLUMN %s.%s IS '%s'"%(tbl_name,col_name,comment)
        try:
            cur.execute(sql)
            db_conn.commit()
        except Exception as e:
            logger.error(" ".join([sql]))
            logger.error(e)
            return False
            
        return True
    
    def save_tbl_comment(self,tbl_name,comment):
        logger.info( " ".join([tbl_name,comment]) )

        conf = self.get_conf()
        db_conn = self.db_connect()
        cur = self.db_cursor(db_conn)
        sql = "COMMENT ON TABLE %s IS '%s'"%(tbl_name,comment)
        try:

            cur.execute(sql)
            db_conn.commit()
        except Exception as e:
            logger.error(" ".join([sql]))
            logger.error(e)
            return False
            
        return True
    

    def del_tbl_rows(self,tbl_name):
        logger.info("start "+ tbl_name )

        conf = self.get_conf()
        db_conn = self.db_connect()
        db_cur = self.db_cursor(db_conn)
        sql = "delete from " + tbl_name
        try:
            db_cur.execute(sql)
            db_conn.commit()
        except Exception as e:
            logger.error(e)
            logger.error(" ".join([sql]))
            return False
            
        return True

    # bulk insert
    def save_tbl_rows(self, tbl_name, atri_keys, rows):
        
        bulk_insert_size = self.get_conf()["common"]["bulk_insert_size"]
        row_groups = self.divide_rows(rows, bulk_insert_size, atri_keys )
        
        sql = "INSERT INTO %s (%s) VALUES %s" % (tbl_name,
                                                 ",".join(atri_keys),"%s")
        
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            for row_group in row_groups:
                try:
                    # bulk insert
                    extras.execute_values(db_cur,sql,row_group)
                except Exception as e:
                    logger.error(e)
                    logger.error(sql)
                    logger.error(row_group)
                    return False
                    
            db_conn.commit()
        return True

    # for bulk insert
    def divide_rows(self, org_rows, chunk_size, atri_keys):
        i = 0
        chunk = []
        ret_rows = []
        for org_row in org_rows:
            new_tuple = ()
            for atri_key in atri_keys:
                new_tuple += (org_row[atri_key],)
            chunk.append( new_tuple )
            
            if len(chunk) >= chunk_size:
                ret_rows.append(chunk)
                chunk = []
            i += 1

        if len(chunk) > 0:
            ret_rows.append(chunk)

        return ret_rows
    
    
    # bulk update or insert
    def bulk_upsert(self, tbl_name, pkeys, all_keys, update_keys, rows):
        
        bulk_insert_size = self.get_conf()["common"]["bulk_insert_size"]
        row_groups = self.divide_rows(rows, bulk_insert_size, all_keys )

        all_keys_str     = ",".join( all_keys )
        
        set_key_vals = [] # UPDATE SQLのSET用
        for atri_key in update_keys:
            set_key_vals.append("%s=tmp.%s" % (atri_key,atri_key) )
        set_key_vals_str = ",".join( set_key_vals)
        
        where_pkeys  = []
        return_pkeys = []
        tmp_pkeys = []
        for pkey in pkeys:
            where_pkeys.append( "tbl_update.%s=tmp.%s"% (pkey,pkey) )
            return_pkeys.append("tbl_update.%s" % (pkey,) )
            tmp_pkeys.append(   "tmp.%s"  % (pkey,) )
            
        where_pkeys_str  = " AND ".join( where_pkeys )
        return_pkeys_str = ",".join( return_pkeys )
        tmp_pkeys_str    = ",".join( tmp_pkeys )
        pkeys_str        = ",".join( pkeys     )

        
# refer to https://qiita.com/yuuuuukou/items/d7723f45e83deb164d68
        sql = """
WITH
tmp( {0} )
AS ( values {1}),
upsert AS ( UPDATE {2} tbl_update
            SET {3}
            FROM tmp
            WHERE {4}
            RETURNING {5} )
INSERT INTO {6} ( {7} )
SELECT {8}
FROM tmp
WHERE ( {9} ) NOT IN ( SELECT {10} FROM UPSERT )
"""
        sql = sql.format(
            all_keys_str,      "%s",            tbl_name,
            set_key_vals_str,   where_pkeys_str,return_pkeys_str,
            tbl_name,           all_keys_str,   all_keys_str,
            tmp_pkeys_str,      pkeys_str )
        
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            for row_group in row_groups:
                try:
                    extras.execute_values(db_cur, sql, row_group )
                except Exception as e:
                    logger.error(e)
                    logger.error(sql)
                    logger.error(row_group)
                    return False
                
        db_conn.commit()
        
        return True
    
    # bulk update or insert
    def bulk_upsert_bak(self, tbl_name, pkeys, atri_keys, rows):

        bulk_insert_size = self.get_conf()["common"]["bulk_insert_size"]
        row_groups = self.divide_rows(rows, bulk_insert_size, atri_keys )

        atri_key_vals = []
        atri_keys_1   = []
        atri_keys_2   = []
        for atri_key in atri_keys:
            atri_key_vals.append("%s=tmp.%s" % (atri_key,atri_key) )
            atri_keys_1.append("tmp.%s"    % (atri_key,) )
            atri_keys_2.append("%s"              % (atri_key,) )

        set_key_vals_str = ",".join( atri_key_vals )
        atri_keys_1_str  = ",".join( atri_keys_1 )
        atri_keys_2_str  = ",".join( atri_keys_2 )
        atri_keys_str = ",".join(atri_keys)
        
        where_pkeys = []
        return_pkeys = []
        tmp_pkeys = []
        raw_pkeys = []
        for pkey in pkeys:
            where_pkeys.append( "tbl_update.%s=tmp.%s"% (pkey,pkey) )
            return_pkeys.append("tbl_update.%s" % (pkey,) )
            tmp_pkeys.append(   "tmp.%s"  % (pkey,) )
            raw_pkeys.append(   "%s"      % (pkey,) )
            
        where_pkeys_str  = " AND ".join( where_pkeys )
        return_pkeys_str = ",".join( return_pkeys )
        tmp_pkeys_str    = ",".join( tmp_pkeys )
        pkeys_str        = ",".join( pkeys     )
        raw_pkeys_str    = ",".join( raw_pkeys )
        
# refer to https://qiita.com/yuuuuukou/items/d7723f45e83deb164d68
        sql = """
WITH
tmp( {0} )
AS ( values {1}),
upsert AS ( UPDATE {2} tbl_update
            SET {3}
            FROM tmp
            WHERE {4}
            RETURNING {5} )
INSERT INTO {6} ( {7} )
SELECT {8}
FROM tmp
WHERE ( {9} ) NOT IN ( SELECT {10} FROM UPSERT )
"""
        sql = sql.format(
            atri_keys_str,     "%s",            tbl_name,
            atri_key_vals_str,  where_pkeys_str,return_pkeys_str,
            tbl_name,           atri_keys_2_str,atri_keys_str,
            tmp_pkeys_str,      raw_pkeys_str )
        
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            for row_group in row_groups:
                try:
                    # bulk upsert
                    extras.execute_values(db_cur,sql, row_group )
                except Exception as e:
                    logger.error(e)
                    logger.error(sql)
                    logger.error(row_group)
                    return False
                    
        db_conn.commit()
        
        return True
    
    # bulk update or insert
    def bulk_update(self, tbl_name, pkeys, atri_keys, rows):

        bulk_insert_size = self.get_conf()["common"]["bulk_insert_size"]
        row_groups = self.divide_rows(rows, bulk_insert_size, atri_keys )
        
# refer to https://qiita.com/yuuuuukou/items/d7723f45e83deb164d68
        sql = """
UPDATE {0}
SET    {1}
FROM ( VALUES {2}) AS data_tbl({3})
WHERE  {4}
"""
        set_key_vals = []
        for atri_key in atri_keys:
            set_key_vals.append("%s=data_tbl.%s" % (atri_key,atri_key) )
            
        where_conds  = []
        for pkey in pkeys:
            where_conds.append("%s.%s=data_tbl.%s" % (tbl_name,pkey,pkey))

        set_key_vals_str = ",".join( set_key_vals )
        atri_key_str     = ",".join( atri_keys )
        where_conds_str  = " AND ".join(where_conds)
        
        sql = sql.format( tbl_name,
                          set_key_vals_str,
                          "%s",
                          atri_key_str,
                          where_conds_str )
        
        db_conn = self.db_connect()
        with self.db_cursor(db_conn) as db_cur:
            for row_group in row_groups:
                try:
                    # bulk upsert
                    extras.execute_values(db_cur,sql, row_group )
                except Exception as e:
                    logger.error(e)
                    logger.error(sql)
                    logger.error(row_group)
                    return False
                    
        db_conn.commit()
        
        return True
    
