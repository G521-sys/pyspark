package com.cdc;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;

public class PgSql {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","root");

        JdbcIncrementalSource<String> postgresIncrergentalSource =
                PostgresSourceBvilder.PostgresIncrementalSource .< String>builder()
                        .hostnane("18.168.68.14")
                        .port(5432)
                        .catabase("spider_db")
                        .schanaList("public")
                        .tableList("public.source_data_car_info_ressage_dtl")
                        .username("etl_flink_cdc_pub_user")
                        .password("etl_flink_cdc_pub_user123, ./")
                        .slotNane("flink_etL_cdc_test")
                        .deserializer(new JsonDebeziunDeserializationSchena())
                        .decodingPluginNane("pgoutput")
                        .includeSchemaChanges(true)
                        .startupOptions(StartupOptions.initiol())
                        .build();

    }
}
