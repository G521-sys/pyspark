package com.example.flink;

public class SQLserver_Flink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode", "schema_only");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource .< String>builder()
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

        I

        DataStreamSource<String> dataStreamSource = env.addSource(sqlServerSource, sourceName: "_transaction_log_source1");
        dataStreamSource.print().setParallelism(1);
        env.execute( jobName: "sqlserver-cdc-test");
    }
}
