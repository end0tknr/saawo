-- postgres=# create database saawo TEMPLATE = template0 ENCODING = 'UTF8';

CREATE TABLE IF NOT EXISTS city (
code            varchar(6),
pref            varchar(4),
city            varchar(8),
lng             double precision,
lat             double precision,
primary key(code));
COMMENT ON COLUMN city.code IS '市町村code';


CREATE TABLE IF NOT EXISTS suumo_search_result_url (
build_type      varchar(32),
url             varchar(256),
primary key(url) );

CREATE TABLE IF NOT EXISTS suumo_bukken (
url             varchar(128),
build_type      varchar(32),
bukken_name     varchar(64),
price           bigint,
price_org       varchar(64),
pref            varchar(4),
city            varchar(8),
address         varchar(128),
plan            varchar(64),
build_area_m2   int,
build_area_org  varchar(64),
land_area_m2    int,
land_area_org   varchar(64),
build_year      int,
shop_org        varchar(64),
shop            varchar(64),
total_house     int,
house_for_sale  int,
found_date      date,
show_date       date,
check_date      date,
update_time     timestamp,
primary key ( url )
);

CREATE TABLE IF NOT EXISTS real_estate_shop (
government              varchar(16),
licence                 varchar(16),
shop                    varchar(64),
primary key(government,licence)
);
COMMENT ON COLUMN real_estate_shop.government
     IS '例; 国土交通大臣、東京都';

CREATE TABLE IF NOT EXISTS mlit_fudousantorihiki (
id                      serial,
trade_year_q            int,
shurui                  varchar(16),
chiiki                  varchar(16),
pref                    varchar(4),
city                    varchar(16),
town                    varchar(64),
station                 varchar(32),
from_station_min        int,
price                   bigint,
plan                    varchar(16),
floor_area_m2           int,
land_area_m2            int,
build_year              int,
structure               varchar(16),
new_usage               varchar(16),
youto_chiiki            varchar(32),
primary key(id) );
COMMENT ON TABLE mlit_fudousantorihiki IS
'https://www.land.mlit.go.jp/webland/download.html';
COMMENT ON COLUMN mlit_fudousantorihiki.trade_year_q
     IS '年度と四半期';
COMMENT ON COLUMN mlit_fudousantorihiki.shurui
     IS '宅地(土地と建物), 宅地(土地), 中古マンション等市町村code';
COMMENT ON COLUMN mlit_fudousantorihiki.chiiki IS '住宅地, 宅地見込地';

CREATE TABLE IF NOT EXISTS mlit_fudousantorihiki_by_city (
pref                    varchar(4),
city                    varchar(16),
newbuild_quarter        varchar(10240),
newbuild_year           varchar(10240),
newbuild_price          varchar(10240),
sumstock_quarter        varchar(10240),
sumstock_year           varchar(10240),
sumstock_price          varchar(10240),
primary key(pref,city) );

CREATE TABLE IF NOT EXISTS mlit_fudousantorihiki_by_town (
pref                    varchar(4),
city                    varchar(16),
town                    varchar(64),
newbuild_quarter        varchar(10240),
newbuild_year           varchar(10240),
newbuild_price          varchar(10240),
sumstock_quarter        varchar(10240),
sumstock_year           varchar(10240),
sumstock_price          varchar(10240),
primary key(pref,city,town) );

CREATE TABLE IF NOT EXISTS newbuild_sales_count_by_shop (
pref                    varchar(4),
shop                    varchar(64),
calc_date               date,
onsale_count            int,
onsale_price            bigint,
onsale_days             int,
discuss_count           int,
discuss_price           bigint,
discuss_days            int,
primary key(pref,shop,calc_date) );

CREATE TABLE IF NOT EXISTS newbuild_sales_count_by_shop_city (
pref                    varchar(4),
city                    varchar(16),
shop                    varchar(64),
calc_date               date,
onsale_count            int,
onsale_price            bigint,
onsale_days             int,
discuss_count           int,
discuss_price           bigint,
discuss_days            int,
primary key(pref,city,shop,calc_date) );

CREATE TABLE IF NOT EXISTS newbuild_sales_count_by_shop_town(
pref                    varchar(4),
city                    varchar(16),
town                    varchar(64),
shop                    varchar(64),
calc_date               date,
onsale_count            int,
onsale_price            bigint,
onsale_days             int,
discuss_count           int,
discuss_price           bigint,
discuss_days            int,
primary key(pref,city,town,shop,calc_date) );

CREATE TABLE IF NOT EXISTS newbuild_sales_count_by_city (
pref                    varchar(4),
city                    varchar(16),
calc_date               date,
onsale_count            int,
onsale_price            bigint,
onsale_days             int,
discuss_count           int,
discuss_price           bigint,
discuss_days            int,
sold_count              numeric,
sold_price              bigint,
primary key(pref,city,calc_date) );

CREATE TABLE IF NOT EXISTS newbuild_sales_count_by_town (
pref                    varchar(4),
city                    varchar(16),
town                    varchar(64),
calc_date               date,
onsale_count            int,
onsale_price            bigint,
onsale_days             int,
discuss_count           int,
discuss_price           bigint,
discuss_days            int,
sold_count              numeric,
sold_price              bigint,
primary key(pref,city,town,calc_date) );

CREATE TABLE IF NOT EXISTS newbuild_sales_count_by_city_price (
pref                    varchar(4),
city                    varchar(16),
price                   int,
calc_date               date,
onsale_count            int,
onsale_days             int,
discuss_count           int,
discuss_days            int,
sold_count              numeric,
sold_count_q            numeric,
primary key(pref,city,price,calc_date) );

CREATE TABLE IF NOT EXISTS newbuild_sales_count_by_shop_scale (
pref                    varchar(4),
shop                    varchar(64),
calc_date               date,
scale_sales             varchar(4096),
primary key(pref,shop,calc_date) );

CREATE TABLE IF NOT EXISTS newbuild_sales_count_by_shop_city_scale (
pref                    varchar(4),
city                    varchar(16),
shop                    varchar(64),
calc_date               date,
scale_sales             varchar(4096),
primary key(pref,city,shop,calc_date) );

CREATE TABLE IF NOT EXISTS newbuild_sales_count_by_city_scale (
pref                    varchar(4),
city                    varchar(16),
calc_date               date,
scale_sales             varchar(4096),
primary key(pref,city,calc_date) );

CREATE TABLE IF NOT EXISTS newbuild_sales_count_by_town_scale (
pref                    varchar(4),
city                    varchar(16),
town                    varchar(64),
calc_date               date,
scale_sales             varchar(4096),
primary key(pref,city,town,calc_date) );
