package com;


public class TestMysqlCDC {
    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Properties debeziumProperties = new Properties();
//        debeziumProperties.put("snapshot.mode", "initial");
//        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
//// MySQL特殊配置：开启gtid模式（如果MySQL启用了GTID）
//         debeziumProperties.put("database.serverTimezone", "UTC");
//         debeziumProperties.put("database.gtid.mode", "enable");
//        DebeziumSourceFunction<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname("192.168.200.128") // MySQL主机地址
//                .port(3306) // MySQL端口号，默认3306
//                .username("root") // MySQL用户名
//                .password("000000") // MySQL密码
//                .databaseList("page") // 要监控的数据库
//                .tableList("page.page_log") // 要监控的表，格式：数据库名.表名，多个表用逗号分隔
//                .startupOptions(StartupOptions.initial()) // 启动选项：从初始位置开始
//                .debeziumProperties(debeziumProperties) // 配置Debezium属性
//                .deserializer(new JsonDebeziumDeserializationSchema()) // 反序列化为JSON格式
//                .build();
//        DataStreamSource<String> dataStreamSource = env.addSource(mySqlSource, "MySQL-CDC-Source");
//
//        // 打印输出结果
//        dataStreamSource.print().setParallelism(1);
//
//        // 缺少的步骤：执行Flink作业
//        env.execute("MySQL CDC Test");
    }
}
