from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum, when, coalesce, avg, lit
)

# 初始化SparkSession（沿用原有配置）
spark = SparkSession.builder \
    .appName("Goods_DWS_Layer") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/home/user/hive/warehouse") \
    .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

# 使用指定数据库
spark.sql("use work_order1")

# ------------------------------
# 1. 商品日度行为汇总表（dws_goods_daily_behavior_summary）
# ------------------------------
# 删除已有表
spark.sql("drop table if exists dws_goods_daily_behavior_summary")

# 创建外部表
spark.sql("""
create external table dws_goods_daily_behavior_summary(
    goods_id bigint COMMENT '商品 ID',
    visitor_count bigint COMMENT '商品访客数（去重）',
    pc_visitor_count bigint COMMENT 'PC 端商品访客数（去重）',
    wireless_visitor_count bigint COMMENT '无线端商品访客数（去重）',
    pv_count bigint COMMENT '商品浏览量',
    total_stay_time int COMMENT '总停留时长（秒）',
    click_count bigint COMMENT '点击详情页人数（去重）',
    collect_user_count bigint COMMENT '商品收藏人数（去重）',
    cart_item_count bigint COMMENT '商品加购件数',
    cart_user_count bigint COMMENT '商品加购人数（去重）',
    order_buyer_count bigint COMMENT '下单买家数（去重）',
    order_quantity bigint COMMENT '下单件数',
    order_amount double COMMENT '下单金额',
    pay_buyer_count bigint COMMENT '支付买家数（去重）',
    pc_pay_buyer_count bigint COMMENT 'PC 端支付买家数（去重）',
    wireless_pay_buyer_count bigint COMMENT '无线端支付买家数（去重）',
    pay_quantity bigint COMMENT '支付件数',
    pay_amount double COMMENT '支付金额',
    juhuasuan_pay_amount double COMMENT '聚划算支付金额'
)
comment '商品日度行为汇总表'
partitioned by (`dt` string COMMENT '统计日期（yyyy - MM - dd）')
row format delimited fields terminated by ','
stored as textfile
location 'hdfs://cdh01:8020/bigdata_warehouse/work/dws/dws_goods_daily_behavior_summary'
tblproperties('textfile.compress' = 'snappy')
""")

# 读取DWD层关联表（过滤2025-08-06数据）
visit_detail = spark.table("dwd_goods_visit_detail").where("dt = '2025-08-06'")
collect_detail = spark.table("dwd_goods_collect_detail").where("dt = '2025-08-06'")
cart_detail = spark.table("dwd_goods_cart_detail").where("dt = '2025-08-06'")
order_detail = spark.table("dwd_order_detail").where("dt = '2025-08-06'")
pay_detail = spark.table("dwd_pay_detail").where("dt = '2025-08-06'")

# 关联表并计算汇总指标
# 关联表并计算汇总指标
goods_daily_summary = (visit_detail.alias("v")
                       .join(collect_detail.alias("c"),
                             (col("v.goods_id") == col("c.goods_id")) & (col("v.dt") == col("c.dt")),
                             "left")
                       # 左连接加购表
                       .join(cart_detail.alias("cart"),
                             (col("v.goods_id") == col("cart.goods_id")) & (col("v.dt") == col("cart.dt")),
                             "left")
                       # 左连接订单表
                       .join(order_detail.alias("o"),
                             (col("v.goods_id") == col("o.goods_id")) & (col("v.dt") == col("o.dt")),
                             "left")
                       # 左连接支付表
                       .join(pay_detail.alias("p"),
                             (col("v.goods_id") == col("p.goods_id")) & (col("v.dt") == col("p.dt")),
                             "left")
                       # 按商品ID分组（处理空值）
                       .groupBy(
    coalesce(
        col("v.goods_id"),
        col("c.goods_id"),
        col("cart.goods_id"),
        col("o.goods_id"),
        col("p.goods_id")
    ).alias("goods_id")
)
                       # 计算指标（与SQL逻辑一致）
                       .agg(
    # 访客数统计
    countDistinct("v.user_id").alias("visitor_count"),
    countDistinct(when(col("v.terminal_type") == "PC端", col("v.user_id"))).alias("pc_visitor_count"),
    countDistinct(when(col("v.terminal_type") == "无线端", col("v.user_id"))).alias("wireless_visitor_count"),

    # 浏览量和停留时间
    count("v.user_id").alias("pv_count"),
    sum("v.stay_time").alias("total_stay_time"),
    countDistinct(when(col("v.is_click"), col("v.user_id"))).alias("click_count"),

    # 收藏统计
    countDistinct("c.user_id").alias("collect_user_count"),

    # 加购统计
    sum("cart.cart_quantity").alias("cart_item_count"),
    countDistinct("cart.user_id").alias("cart_user_count"),

    # 订单统计
    countDistinct("o.user_id").alias("order_buyer_count"),
    sum("o.order_quantity").alias("order_quantity"),
    sum("o.order_amount").alias("order_amount"),

    # 支付统计
    countDistinct("p.user_id").alias("pay_buyer_count"),
    countDistinct(when(col("p.terminal_type") == "PC端", col("p.user_id"))).alias("pc_pay_buyer_count"),
    countDistinct(when(col("p.terminal_type") == "无线端", col("p.user_id"))).alias("wireless_pay_buyer_count"),
    sum("p.pay_quantity").alias("pay_quantity"),
    sum("p.pay_amount").alias("pay_amount"),
    sum(when(col("p.activity_type") == "聚划算", col("p.pay_amount")).otherwise(0)).alias("juhuasuan_pay_amount")
)
                       # 添加分区字段
                       .withColumn("dt", lit("2025-08-06"))
                       )


# 写入DWS表
goods_daily_summary.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .saveAsTable("dws_goods_daily_behavior_summary")

print("dws_goods_daily_behavior_summary 数据写入完成")


# ------------------------------
# 2. 商品类目日度汇总表（dws_category_daily_summary）
# ------------------------------
# 删除已有表
spark.sql("drop table if exists dws_category_daily_summary")

# 创建外部表
spark.sql("""
create external table dws_category_daily_summary(
    category_id bigint COMMENT '叶子类目 ID',
    goods_id bigint COMMENT '商品 ID',
    price double COMMENT '商品价格',
    pay_quantity bigint COMMENT '支付件数',
    pay_amount double COMMENT '支付金额',
    new_buyer_count bigint COMMENT '支付新买家数',
    old_buyer_count bigint COMMENT '支付老买家数'
)
comment '商品类目日度汇总表'
partitioned by (`dt` string COMMENT '统计日期（yyyy - MM - dd）')
row format delimited fields terminated by ','
stored as textfile
location 'hdfs://cdh01:8020/bigdata_warehouse/work/dws/dws_category_daily_summary'
tblproperties('textfile.compress' = 'snappy')
""")

# 读取支付表数据（当前日期和历史数据）
current_pay = spark.table("dwd_pay_detail").where("dt = '2025-08-06'")
history_pay = spark.table("dwd_pay_detail").where("dt < '2025-08-06'")

# 关联计算新老买家指标
# 关联计算新老买家指标
category_daily_summary = (current_pay.alias("p")
                          .join(history_pay.alias("hist"),
                                (col("p.user_id") == col("hist.user_id")) &
                                (col("p.goods_id") == col("hist.goods_id")),
                                "left")
                          # 按类目和商品ID分组
                          .groupBy(
    col("p.category_id"),
    col("p.goods_id")
)
                          # 计算指标（与SQL逻辑一致）
                          .agg(
    # 商品价格（支付金额/支付件数的平均值）
    avg(col("p.pay_amount") / col("p.pay_quantity")).alias("price"),
    # 支付件数和金额
    sum("p.pay_quantity").alias("pay_quantity"),
    sum("p.pay_amount").alias("pay_amount"),
    # 新买家数（历史无购买记录）
    countDistinct(when(col("hist.user_id").isNull(), col("p.user_id"))).alias("new_buyer_count"),
    # 老买家数（历史有购买记录）
    countDistinct(when(col("hist.user_id").isNotNull(), col("p.user_id"))).alias("old_buyer_count")
)
                          # 添加分区字段
                          .withColumn("dt", lit("2025-08-06"))
                          )

# 写入DWS表
category_daily_summary.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .saveAsTable("dws_category_daily_summary")

print("dws_category_daily_summary 数据写入完成")

# 停止SparkSession
spark.stop()