from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, round, sum, expr, rank, current_timestamp,
    lit, countDistinct,date_sub,current_date
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 初始化SparkSession（沿用数仓环境配置）
spark = SparkSession.builder \
    .appName("Goods_ADS_Layer") \
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
# 1. 商品效率监控表（ads_goods_efficiency）
# ------------------------------
# 删除已有表
spark.sql("drop table if exists ads_goods_efficiency")

# 创建外部表
spark.sql("""
create external table ads_goods_efficiency(
    goods_id bigint COMMENT '商品 ID',
    conversion_rate double COMMENT '转化率',
    efficiency_score double COMMENT '效率得分'
)
comment '商品效率监控表'
partitioned by (`dt` string COMMENT '统计日期（yyyy - MM - dd）')
row format delimited fields terminated by ','
stored as textfile
location 'hdfs://cdh01:8020/bigdata_warehouse/work/ads/ads_goods_efficiency'
tblproperties('textfile.compress' = 'snappy')
""")

# 读取DWS层关联表
goods_daily = spark.table("dws_goods_daily_behavior_summary").where("dt = '2025-08-06'")
category_daily = spark.table("dws_category_daily_summary").where("dt = '2025-08-06'")

# 计算转化率和效率得分
efficiency_df = goods_daily.alias("g") \
    .join(category_daily.alias("p"), on="goods_id", how="inner") \
    .withColumn(
    "conversion_rate",
    when(col("g.visitor_count") == 0, 0)
        .otherwise(round(col("p.old_buyer_count") / col("g.visitor_count"), 4))
) \
    .withColumn(
    "total_pay_amount",
    sum("p.pay_amount").over(Window.partitionBy())  # 全局支付金额总和
) \
    .withColumn(
    "efficiency_score",
    round(
        (when(col("g.visitor_count") == 0, 0)
         .otherwise(col("p.old_buyer_count") / col("g.visitor_count")) * 100) +
        (col("p.pay_amount") / col("total_pay_amount") * 50),
        2
    )
) \
    .select(
    col("goods_id"),
    col("conversion_rate"),
    col("efficiency_score"),
    lit("2025-08-06").alias("dt")
)

# 写入数据
efficiency_df.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .saveAsTable("ads_goods_efficiency")

print("ads_goods_efficiency 数据写入完成")


# ------------------------------
# 2. 商品区间分析表（ads_goods_interval_analysis）
# ------------------------------
# 删除已有表
spark.sql("drop table if exists ads_goods_interval_analysis")

# 创建外部表
spark.sql("""
create external table ads_goods_interval_analysis(
    goods_id bigint COMMENT '商品 ID',
    interval_type string COMMENT '区间类型（如价格区间、销量区间等）',
    interval_start double COMMENT '区间起始值',
    interval_end double COMMENT '区间结束值',
    indicator_value double COMMENT '区间指标值（如该区间销量、占比等）'
)
comment '商品区间分析表'
partitioned by (`dt` string COMMENT '统计日期（yyyy - MM - dd）')
row format delimited fields terminated by ','
stored as textfile
location 'hdfs://cdh01:8020/bigdata_warehouse/work/ads/ads_goods_interval_analysis'
tblproperties('textfile.compress' = 'snappy')
""")

# 读取DWS类目表数据
# 读取DWS类目表数据
category_data = spark.table("dws_category_daily_summary").where("dt = '2025-08-06'")

# 定义价格区间并计算指标
interval_df = category_data \
    .withColumn(
    "interval_start",
    F.when(col("price") < 50, 0)
        .when((col("price") >= 50) & (col("price") < 100), 50)
        .when((col("price") >= 100) & (col("price") < 200), 100)
        .when((col("price") >= 200) & (col("price") < 500), 200)
        .otherwise(500)
) \
    .withColumn(
    "interval_end",
    F.when(col("price") < 50, 50)
        .when((col("price") >= 50) & (col("price") < 100), 100)
        .when((col("price") >= 100) & (col("price") < 200), 200)
        .when((col("price") >= 200) & (col("price") < 500), 500)
        .otherwise(9999)
) \
    .groupBy(
    col("goods_id"),
    col("interval_start"),
    col("interval_end")
) \
    .agg(
    F.sum("pay_amount").alias("indicator_value")
) \
    .withColumn("interval_type", lit("price")) \
    .withColumn("dt", lit("2025-08-06")) \
    .select(
    "goods_id", "interval_type", "interval_start",
    "interval_end", "indicator_value", "dt"
)

# 写入数据
interval_df.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .saveAsTable("ads_goods_interval_analysis")

print("ads_goods_interval_analysis 数据写入完成")


# ------------------------------
# 3. 商品销售排行表（ads_goods_sale_ranking）
# ------------------------------
# 删除已有表
spark.sql("drop table if exists ads_goods_sale_ranking")

# 创建表（使用parquet格式）
spark.sql("""
create table ads_goods_sale_ranking(
    goods_id bigint comment '商品ID',
    stat_period string comment '统计周期（如7d/30d/month）',
    stat_type string comment '周期类型（day/week/month/custom）',
    sales_volume bigint comment '销量（支付件数总和）',
    sales_amount double comment '销售额（支付金额总和）',
    pay_conversion_rate double comment '支付转化率（支付买家数/商品访客数）',
    ranking_by_volume int comment '销量排名',
    ranking_by_amount int comment '销售额排名',
    pc_sales_ratio double comment 'PC端销售占比',
    wireless_sales_ratio double comment '无线端销售占比',
    create_time string comment '数据生成时间'
)
comment '商品销售排行表'
partitioned by (`dt` string COMMENT '统计日期（yyyy - MM - dd）')
row format delimited fields terminated by ','
stored as textfile
location 'hdfs://cdh01:8020/bigdata_warehouse/work/ads/ads_goods_sale_ranking'
tblproperties('textfile.compress' = 'snappy')
""")

# 计算近7天数据（使用date_sub获取当前日期前7天）
# 注：实际环境中current_date()为运行当天，此处为示例固定为2025-08-06
dws_df = spark.table("dws_goods_daily_behavior_summary") \
    .where(col("dt") >= date_sub(current_date(), 7))  # 筛选近7天数据

# 聚合计算
agg_df = dws_df.groupBy(col("goods_id")) \
    .agg(
    sum("pay_quantity").alias("sales_volume"),
    sum("pay_amount").alias("sales_amount"),
    sum("pay_buyer_count").alias("total_pay_buyer"),
    sum("visitor_count").alias("total_visitor"),
    sum("pc_pay_buyer_count").alias("total_pc_buyer"),
    sum("wireless_pay_buyer_count").alias("total_wireless_buyer")
)

# 计算比率和排名
result_df = agg_df \
    .withColumn(
    "pay_conversion_rate",
    when(col("total_visitor") == 0, 0)
        .otherwise(round(col("total_pay_buyer") / col("total_visitor"), 4))
) \
    .withColumn(
    "pc_sales_ratio",
    when(col("total_pay_buyer") == 0, 0)
        .otherwise(round(col("total_pc_buyer") / col("total_pay_buyer"), 4))
) \
    .withColumn(
    "wireless_sales_ratio",
    when(col("total_pay_buyer") == 0, 0)
        .otherwise(round(col("total_wireless_buyer") / col("total_pay_buyer"), 4))
) \
    .withColumn(
    "ranking_by_volume",
    rank().over(Window.orderBy(col("sales_volume").desc()))
) \
    .withColumn(
    "ranking_by_amount",
    rank().over(Window.orderBy(col("sales_amount").desc()))
) \
    .withColumn("stat_period", lit("7d")) \
    .withColumn("stat_type", lit("week")) \
    .withColumn("create_time", current_timestamp().cast("string")) \
    .withColumn("dt", current_date().cast("string"))  # 添加分区字段dt

# 选择最终字段
final_df = result_df.select(
    col("goods_id"),
    col("stat_period"),
    col("stat_type"),
    col("sales_volume"),
    col("sales_amount"),
    col("pay_conversion_rate"),
    col("ranking_by_volume"),
    col("ranking_by_amount"),
    col("pc_sales_ratio"),
    col("wireless_sales_ratio"),
    col("create_time"),
    col("dt")  # 包含分区字段
)

# 写入数据（追加模式）
final_df.write \
    .mode("append") \
    .partitionBy("dt") \
    .format("hive") \
    .saveAsTable("ads_goods_sale_ranking")

print("ads_goods_sale_ranking 表创建并插入数据完成")

print("ads_goods_sale_ranking 数据写入完成")

# 停止SparkSession
spark.stop()