from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import sys
def main():
    spark = SparkSession.builder \
        .appName("Hive_Dimension_Load") \
        .master("local[*]") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.warehouse.dir", "/bigdata_warehouse/tms") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # 1. 创建外部表（修正：去掉无意义的 .show()）
    create_table_sql = """
    create external table dim_organ_full(
        id              string comment '机构ID',
        org_name        string comment '机构名称',
        org_level       string comment '机构等级(1为转运中心，2转运站)',
        region_id       string comment '地区ID，1级机构为city，2级机构为district',
        region_name     string comment '地区名称',
        region_code     string comment '地区编码（行政级别）',
        org_parent_id   string comment '父级机构ID',
        org_parent_mame string comment '父级机构名称'
    )comment '机构维度表'
    partitioned by (dt string)
    stored as orc
    location 'hdfs://cdh01:8088/bigdata_warehouse/tms/dim/dim_organ_full'
    tblproperties ('orc.compress'='snappy');
    """
    spark.sql(create_table_sql)

    # 2. insert overwrite 语句（修正 JOIN 条件）
    insert_sql = """
    with cx as (
        select id, complex_name, province_id, city_id, district_id, district_name
        from ods_base_complex
        where is_deleted = 0
    ),
    pv as (
        select id, name
        from ods_base_region_info
        where is_deleted = 0
    ),
    cy as (
        select id, name
        from ods_base_region_info
        where is_deleted = 0
    ),
    ex as (
        select collect_set(cast(courier_emp_id as string)) courier_emp_id, complex_id
        from ods_express_courier_complex 
        where is_deleted = 0
        group by complex_id
    )
    insert overwrite table dim_complex_full partition (dt='2025-07-12')
    select 
        cx.id,
        cx.complex_name,
        ex.courier_emp_id,
        cx.province_id,
        pv.name,
        cx.city_id,
        cy.name,
        cx.district_id,
        cx.district_name
    from cx
    left join pv on cx.province_id = pv.id
    left join cy on cx.city_id = cy.id  -- 修正为 cy.id
    left join ex on cx.id = ex.complex_id;
    """
    spark.sql(insert_sql)

if __name__ == '__main__':
    main()