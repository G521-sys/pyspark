from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum, when, coalesce, round,
    current_timestamp, date_sub, rank, lit
)

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("Hive_Dimension_Load") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/home/user/hive/warehouse") \
    .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

# ------------------------------
# 1. 创建数据库
# ------------------------------
spark.sql("create database if not exists work_order1")
spark.sql("use work_order1")

# ------------------------------
# 2. ODS层表结构定义与数据加载
# ------------------------------
# 2.1 ODS用户行为日志表
# spark.sql("drop table if exists ods_user_behavior_log")
# spark.sql("""
# create external table ods_user_behavior_log(
#     log_id bigint COMMENT '日志 ID',
#     user_id bigint COMMENT '用户 ID',
#     goods_id bigint COMMENT '商品 ID',
#     behavior_type string COMMENT '行为类型（访问、收藏、加购等）',
#     behavior_time string COMMENT '行为时间',
#     terminal_type string COMMENT '终端类型',
#     page_stay_time int COMMENT '页面停留时间',
#     is_click boolean COMMENT '是否点击详情页'
# )
# comment '用户行为日志表'
# partitioned by (dt string)
# row format delimited fields terminated by ','
# stored as textfile
# location 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_user_behavior_log'
# tblproperties('textfile.compress' = 'snappy')
# """)
# spark.sql("""
# load data inpath 'hdfs://cdh01:8020/work/ods_user_behavior_log.txt'
# into table ods_user_behavior_log partition (dt='2025-08-06')
# """)
#
#
# ## 2.2 ODS订单表
# spark.sql("drop table if exists ods_order")
# spark.sql("""
# create external table ods_order(
#     order_id bigint, user_id bigint, goods_id bigint, category_id bigint,
#     order_time string, pay_time string, order_amount double, pay_amount double,
#     order_quantity int, pay_quantity int, terminal_type string,
#     is_paid boolean, activity_type string
# )
# comment '订单表'
# partitioned by (dt string)
# row format delimited fields terminated by ','
# stored as textfile
# location 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_order'
# tblproperties('textfile.compress' = 'snappy')
# """)
# spark.sql("""
# load data local inpath 'hdfs://cdh01:8020/work/ods_order.csv'
# into table ods_order partition (dt='2025-08-06')
# """)
#
#
# ## 2.3 ODS商品信息表
# spark.sql("drop table if exists ods_goods_info")
# spark.sql("""
# create external table ods_goods_info(
#     goods_id bigint, category_id bigint, price double
# )
# comment '商品信息表'
# partitioned by (dt string)
# row format delimited fields terminated by ','
# stored as textfile
# location 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_goods_info'
# """)
# spark.sql("""
# load data local inpath 'hdfs://cdh01:8020/work/ods_goods_info.csv'
# into table ods_goods_info partition (dt='2025-08-06')
# """)

# ------------------------------
# 3. DWD层表处理
# ------------------------------

## 3.1 商品访问行为明细表
spark.sql("drop table if exists dwd_goods_visit_detail")
spark.sql("""
create external table dwd_goods_visit_detail(
    user_id bigint, goods_id bigint, visit_time string,
    terminal_type string, stay_time int, is_click boolean
)
comment '商品访问行为明细表'
partitioned by (dt string)
stored as orc
location 'hdfs://cdh01:8020/bigdata_warehouse/work/dwd/dwd_goods_visit_detail'
tblproperties('orc.compress' = 'snappy')
""")
spark.table("ods_user_behavior_log") \
    .where("dt='2025-08-06' and behavior_type='Visit'") \
    .select(
    col("user_id"), col("goods_id"),
    col("behavior_time").alias("visit_time"),
    col("terminal_type"),
    col("page_stay_time").alias("stay_time"),
    col("is_click"), col("dt")
) \
    .write.mode("overwrite")\
    .partitionBy("dt")\
    .saveAsTable("dwd_goods_visit_detail")


## 3.2 商品收藏/加购行为表（合并处理）
# 收藏表
spark.sql("drop table if exists dwd_goods_collect_detail")
spark.sql("""
create external table dwd_goods_collect_detail(
    user_id bigint, goods_id bigint, collect_time string
)
comment '商品收藏行为明细表'
partitioned by (dt string)
stored as orc
location 'hdfs://cdh01:8020/bigdata_warehouse/work/dwd/dwd_goods_collect_detail'
""")
spark.table("ods_user_behavior_log") \
    .where("dt='2025-08-06' and behavior_type='Favorite'") \
    .select(
    col("user_id"), col("goods_id"),
    col("behavior_time").alias("collect_time"), col("dt")
) \
    .write.mode("overwrite")\
    .partitionBy("dt")\
    .saveAsTable("dwd_goods_collect_detail")

# 加购表
spark.sql("drop table if exists dwd_goods_cart_detail")
spark.sql("""
create external table dwd_goods_cart_detail(
    user_id bigint, goods_id bigint, cart_time string, cart_quantity int
)
comment '商品加购行为明细表'
partitioned by (dt string)
stored as orc
location 'hdfs://cdh01:8020/bigdata_warehouse/work/dwd/dwd_goods_cart_detail'
""")
spark.table("ods_user_behavior_log") \
    .where("dt='2025-08-06' and behavior_type='AddToCart'") \
    .select(
    col("user_id"), col("goods_id"),
    col("behavior_time").alias("cart_time"),
    lit(1).alias("cart_quantity"), col("dt")
) \
    .write.mode("overwrite")\
    .partitionBy("dt")\
    .saveAsTable("dwd_goods_cart_detail")


## 3.3 订单/支付明细表
# 订单表
spark.sql("drop table if exists dwd_order_detail")
spark.sql("""
create external table dwd_order_detail(
    order_id bigint, user_id bigint, goods_id bigint, category_id bigint,
    order_time string, order_amount double, order_quantity int, terminal_type string
)
comment '订单明细表'
partitioned by (dt string)
stored as orc
location 'hdfs://cdh01:8020/bigdata_warehouse/work/dwd/dwd_order_detail'
""")

spark.table("ods_order")\
    .where("dt='2025-08-06'") \
    .select(
    col("order_id"), col("user_id"), col("goods_id"), col("category_id"),
    col("order_time"), col("order_amount"), col("order_quantity"),
    col("terminal_type"), col("dt")
) \
    .write.mode("overwrite")\
    .partitionBy("dt")\
    .saveAsTable("dwd_order_detail")

# 支付表
spark.sql("drop table if exists dwd_pay_detail")
spark.sql("""
create external table dwd_pay_detail(
    order_id bigint, user_id bigint, goods_id bigint, category_id bigint,
    pay_time string, pay_amount double, pay_quantity int,
    terminal_type string, activity_type string
)
comment '支付明细表'
partitioned by (dt string)
stored as orc
location 'hdfs://cdh01:8020/bigdata_warehouse/work/dwd/dwd_pay_detail'
""")
spark.table("ods_order") \
    .where("dt='2025-08-06' and is_paid=true") \
    .select(
    col("order_id"), col("user_id"), col("goods_id"), col("category_id"),
    col("pay_time"), col("pay_amount"), col("pay_quantity"),
    col("terminal_type"), col("activity_type"), col("dt")
) \
    .write.mode("overwrite")\
    .partitionBy("dt")\
    .saveAsTable("dwd_pay_detail")

#停止SparkSession
spark.stop()