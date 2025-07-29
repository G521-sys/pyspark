package com;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;


/**
 * @Package com.v3.TestSqlServerCDC
 * @Author zhou.han
 * @Date 2025/7/23 15:08
 * @description:
 */
public class TestSqlServerCDC {

    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode", "schema_only");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("10.160.60.19")
                .port(1433)
                .username("sa")
                .password("zh1028,./")
                .database("realtime_v3")
                .tableList("dbo.cdc_test")
                .startupOptions(StartupOptions.latest())
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        DataStreamSource<String> dataStreamSource = env.addSource(sqlServerSource, "_transaction_log_source1");
        dataStreamSource.print().setParallelism(1);
        env.execute("sqlserver-cdc-test");

    }

}