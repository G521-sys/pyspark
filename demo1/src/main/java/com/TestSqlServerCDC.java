package com;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;


/**
 * @Package com.v3.TestSqlServerCDC
 * @Author zhou.han
 * @Date 2025/7/31
 * @description:
 */
public class TestSqlServerCDC {

    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties debeziumProperties = new Properties();
//        debeziumProperties.put("snapshot.mode", "schema_only");
//        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.put("snapshot.mode","initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl","true");
        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("192.168.200.130")
                .port(1433)
                .username("sa")
                .password("zh108,./")
                .database("page")
                .tableList("dbo.page_log")
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        DataStreamSource<String> dataStreamSource = env.addSource(sqlServerSource, "_transaction_log_source1");
        dataStreamSource.print().setParallelism(1);
        env.execute("sqlserver-cdc-test");

    }

}