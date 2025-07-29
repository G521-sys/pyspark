package com;

// 使用本地 SQL Server 实例进行测试
SqlServerSource<String> sqlServerSource = SqlServerSource.<String>builder()
        .hostname("localhost") // 或 "127.0.0.1"
        .port(1433)
        .username("sa")
        .password("your_local_password")
        .database("master")
        .tableList("dbo.your_table")
        .startupOptions(StartupOptions.initial())
        .deserializer(new JsonDebeziumDeserializationSchema())
        .build();
