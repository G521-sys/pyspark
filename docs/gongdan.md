CREATE TABLE page_log (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    log_time DATETIME NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    session_id VARCHAR(50) NOT NULL,
    store_id VARCHAR(50) NOT NULL,
    item_id VARCHAR(50) NULL,
    page_type VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    referrer VARCHAR(200) NULL,
    ip_address VARCHAR(50) NULL,
    device_type VARCHAR(50) NULL,
    os_type VARCHAR(50) NULL,
    browser_type VARCHAR(50) NULL,
    page_stay_time INT NULL
);


-- 创建索引加速查询（如果之前没创建，可保留；若已创建可注释掉）
CREATE NONCLUSTERED INDEX idx_page_log_store_item ON page_log (store_id, item_id);
CREATE NONCLUSTERED INDEX idx_page_log_time ON page_log (log_time DESC);
CREATE NONCLUSTERED INDEX idx_page_log_event ON page_log (event_type);

-- 创建存储过程生成测试数据
CREATE OR ALTER PROCEDURE GeneratePageLogTestData
    @RowCount INT = 1000000 -- 要生成的总记录数
AS
BEGIN
    SET NOCOUNT ON;
    -- 控制批次大小，避免事务过大
    DECLARE @BatchSize INT = 10000;
    DECLARE @TotalInserted INT = 0;

    -- 预定义基础数据（可扩展更多枚举值）
    DECLARE @UserIDs TABLE (UserID VARCHAR(50));
    DECLARE @SessionIDs TABLE (SessionID VARCHAR(50));
    DECLARE @StoreIDs TABLE (StoreID VARCHAR(50));
    DECLARE @PageTypes TABLE (PageType VARCHAR(50));
    DECLARE @EventTypes TABLE (EventType VARCHAR(50));
    DECLARE @DeviceTypes TABLE (DeviceType VARCHAR(50));
    DECLARE @OSTypes TABLE (OSType VARCHAR(50));
    DECLARE @BrowserTypes TABLE (BrowserType VARCHAR(50));

    -- 填充基础枚举数据（可根据业务实际值扩展）
    INSERT INTO @PageTypes (PageType) VALUES
        ('home'), ('product_list'), ('product_detail'), ('checkout'), ('cart');
    INSERT INTO @EventTypes (EventType) VALUES
        ('click'), ('view'), ('scroll'), ('stay'), ('exit');
    INSERT INTO @DeviceTypes (DeviceType) VALUES
        ('mobile'), ('desktop'), ('tablet');
    INSERT INTO @OSTypes (OSType) VALUES
        ('iOS'), ('Android'), ('Windows'), ('macOS'), ('Linux');
    INSERT INTO @BrowserTypes (BrowserType) VALUES
        ('Chrome'), ('Safari'), ('Firefox'), ('Edge'), ('Opera');

    -- 生成随机 UserID（示例：固定 1000 个用户循环）
    WHILE (SELECT COUNT(*) FROM @UserIDs) < 1000
    BEGIN
        INSERT INTO @UserIDs (UserID)
        VALUES ('User_' + RIGHT('00000' + CAST(ABS(CHECKSUM(NEWID())) % 1000 AS VARCHAR(5)), 5));
    END

    -- 生成随机 StoreID（示例：固定 50 个店铺循环）
    WHILE (SELECT COUNT(*) FROM @StoreIDs) < 50
    BEGIN
        INSERT INTO @StoreIDs (StoreID)
        VALUES ('Store_' + RIGHT('0000' + CAST(ABS(CHECKSUM(NEWID())) % 50 AS VARCHAR(2)), 2));
    END

    -- 生成随机 SessionID（每个会话模拟 10 条日志）
    WHILE (SELECT COUNT(*) FROM @SessionIDs) < (@RowCount / 10)
    BEGIN
        INSERT INTO @SessionIDs (SessionID)
        VALUES (NEWID());
    END

    -- 循环插入数据
    WHILE @TotalInserted < @RowCount
    BEGIN
        DECLARE @BatchRows INT = IIF((@RowCount - @TotalInserted) < @BatchSize, @RowCount - @TotalInserted, @BatchSize);

        INSERT INTO page_log (
            log_time,
            user_id,
            session_id,
            store_id,
            item_id,
            page_type,
            event_type,
            referrer,
            ip_address,
            device_type,
            os_type,
            browser_type,
            page_stay_time
        )
        SELECT
            -- 随机时间（过去 30 天内）
            DATEADD(SECOND, -ABS(CHECKSUM(NEWID())) % 2592000, GETDATE()),
            -- 随机用户（从预生成的 1000 个中选）
            (SELECT UserID FROM @UserIDs ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 1 ROW ONLY),
            -- 随机会话（从预生成的 Session 中选）
            (SELECT SessionID FROM @SessionIDs ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 1 ROW ONLY),
            -- 随机店铺（从预生成的 50 个中选）
            (SELECT StoreID FROM @StoreIDs ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 1 ROW ONLY),
            -- 随机商品（可为 NULL，模拟无商品场景）
            IIF(ABS(CHECKSUM(NEWID())) % 2 = 0, 'Item_' + CAST(ABS(CHECKSUM(NEWID())) % 10000 AS VARCHAR(5)), NULL),
            -- 随机页面类型
            (SELECT PageType FROM @PageTypes ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 1 ROW ONLY),
            -- 随机事件类型
            (SELECT EventType FROM @EventTypes ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 1 ROW ONLY),
            -- 随机来源（简单模拟）
            IIF(ABS(CHECKSUM(NEWID())) % 2 = 0, 'https://example.com/referrer_' + CAST(ABS(CHECKSUM(NEWID())) % 1000 AS VARCHAR(4)), NULL),
            -- 随机 IP（简单模拟）
            '192.168.' + CAST(ABS(CHECKSUM(NEWID())) % 255 AS VARCHAR(3)) + '.' + CAST(ABS(CHECKSUM(NEWID())) % 255 AS VARCHAR(3)),
            -- 随机设备类型
            (SELECT DeviceType FROM @DeviceTypes ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 1 ROW ONLY),
            -- 随机操作系统
            (SELECT OSType FROM @OSTypes ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 1 ROW ONLY),
            -- 随机浏览器
            (SELECT BrowserType FROM @BrowserTypes ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 1 ROW ONLY),
            -- 随机停留时间（0-300 秒）
            ABS(CHECKSUM(NEWID())) % 301
        FROM
            -- 生成批量数据（通过 CROSS JOIN 控制行数）
            (SELECT TOP (@BatchRows) 1 AS dummy_column FROM sys.objects o1 CROSS JOIN sys.objects o2) AS T;
        SET @TotalInserted += @BatchRows;
    END
END
GO

-- 执行存储过程生成 100 万条数据
EXEC GeneratePageLogTestData @RowCount = 1000000;

CREATE TABLE page_log (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    log_time DATETIME NOT NULL,               -- 日志时间
    user_id VARCHAR(50) NULL,                 -- 用户ID
    session_id VARCHAR(50) NOT NULL,          -- 会话ID
    store_id VARCHAR(50) NOT NULL,            -- 店铺ID
    item_id VARCHAR(50) NULL,                 -- 商品ID
    page_type VARCHAR(50) NOT NULL,           -- 页面类型（home, item, category等）
    event_type VARCHAR(50) NOT NULL,          -- 事件类型（click, view等）
    referrer_page_id VARCHAR(50) NULL,        -- 来源页面ID
    page_stay_time INT NULL                   -- 页面停留时间（秒）
);

-- 流量主题数仓建设10-流量主题页面分析看板
-- 1. 流量-页面分析
--     1.1 可以查询店铺页面的点击、访问等数据情况
select store_id,sum(case event_type when 'click' then 1 else 0 end ),
       sum(case event_type when 'view' then 1 else 0 end )
from page_log
where page_type='home' group by store_id;
-- 数据治理平台Dataphin搭建09-流量主题店内路径看板


-- 流量主题：店内路径分析表
CREATE TABLE [dbo].[traffic_store_path_analysis] (
    -- 1. 基础标识
    [id] BIGINT IDENTITY(1,1) PRIMARY KEY,  -- 自增主键，唯一标识每条访问记录
    [log_uuid] VARCHAR(64) NOT NULL,       -- 日志唯一 UUID，用于去重和跨系统关联
    [user_id] VARCHAR(50) NOT NULL,        -- 用户唯一标识（如会员 ID/设备 ID）
    [session_id] VARCHAR(50) NOT NULL,     -- 会话 ID，标识一次连续访问行为
    -- 2. 店铺与商品维度
    [store_id] VARCHAR(50) NOT NULL,       -- 店铺 ID，关联店铺维度表
    [item_id] VARCHAR(50) NULL,            -- 商品 ID（访问商品页时填充）
    [category_id] VARCHAR(50) NULL,        -- 分类 ID（访问分类页时填充）
    -- 3. 页面路径信息
    [current_page_code] VARCHAR(50) NOT NULL,  -- 当前页面编码（如 HOME、ITEM_DETAIL）
    [current_page_name] VARCHAR(50) NOT NULL,  -- 当前页面名称（如“首页”“商品详情”）
    [prev_page_code] VARCHAR(50) NULL,         -- 上一页面编码（无则为 NULL）
    [prev_page_name] VARCHAR(50) NULL,         -- 上一页面名称（无则为 NULL）
    -- 4. 行为与时间维度
    [event_type] VARCHAR(20) NOT NULL,     -- 事件类型：VIEW（浏览）、CLICK（点击）、EXIT（离开）等
    [event_action] VARCHAR(50) NULL,       -- 具体行为：如“点击加入购物车”“滚动加载更多”
    [access_time] DATETIME NOT NULL,       -- 访问时间（精确到毫秒）
    [stay_duration] INT NULL,              -- 停留时长（秒，EXIT 事件必填）
    -- 5. 渠道与环境维度
    [referrer_type] VARCHAR(20) NULL,      -- 来源类型：DIRECT（直接访问）、SEARCH（搜索）、AD（广告）等
    [traffic_source] VARCHAR(50) NULL,     -- 流量来源：如“百度搜索”“抖音广告”
    [device_type] VARCHAR(20) NOT NULL,    -- 设备类型：MOBILE（手机）、PC（电脑）、PAD（平板）
    [os_type] VARCHAR(20) NOT NULL,        -- 操作系统：iOS、Android、Windows
    [browser_type] VARCHAR(20) NOT NULL,   -- 浏览器：Chrome、Safari、微信浏览器
    -- 6. 地理与网络维度
    [ip_address] VARCHAR(50) NOT NULL,     -- 访问 IP（用于解析地域）
    [region_code] VARCHAR(20) NULL,        -- 地域编码（如省份编码 110000 代表北京）
    [city_name] VARCHAR(50) NULL,          -- 城市名称（如“北京市”）
    -- 7. 数据治理元数据
    [data_quality_flag] TINYINT NOT NULL DEFAULT 1,  -- 数据质量标记：1（有效）、0（无效）
    [etl_time] DATETIME NOT NULL DEFAULT GETDATE()    -- 数据入仓时间
);
-- 添加字段注释（SQL Server 通过扩展属性实现）
EXEC sp_addextendedproperty
@name = N'MS_Description', @value = N'自增主键',
@level0type = N'SCHEMA', @level0name = N'dbo',
@level1type = N'TABLE', @level1name = N'traffic_store_path_analysis',
@level2type = N'COLUMN', @level2name = N'id';
EXEC sp_addextendedproperty
@name = N'MS_Description', @value = N'用户唯一标识',
@level0type = N'SCHEMA', @level0name = N'dbo',
@level1type = N'TABLE', @level1name = N'traffic_store_path_analysis',
@level2type = N'COLUMN', @level2name = N'user_id';
CREATE OR ALTER PROCEDURE GenerateTrafficStorePathData
    @RowCount INT = 1000000
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    -- 1. 参数校验
    IF @RowCount <= 0
    BEGIN
        RAISERROR('@RowCount 必须为正整数', 16, 1);
        RETURN;
    END;

    -- 2. 配置变量
    DECLARE @BatchSize INT = 100000;
    DECLARE @TotalInserted INT = 0;
    DECLARE @DBName SYSNAME = DB_NAME();

    -- 3. 临时表（防重复创建）
    DROP TABLE IF EXISTS #Pages;
    DROP TABLE IF EXISTS #Events;
    DROP TABLE IF EXISTS #Devices;
    DROP TABLE IF EXISTS #Referrers;
    DROP TABLE IF EXISTS #Regions;

    -- 4. 预定义枚举数据
    CREATE TABLE #Pages (PageCode VARCHAR(50), PageName VARCHAR(50));
    INSERT INTO #Pages VALUES
        ('HOME', '首页'), ('ITEM_LIST', '商品列表'), ('ITEM_DETAIL', '商品详情'),
        ('CART', '购物车'), ('CHECKOUT', '结算页'), ('MEMBER_CENTER', '会员中心'),
        ('HELP', '帮助中心'), ('ORDER_LIST', '订单列表');

    CREATE TABLE #Events (EventType VARCHAR(20));
    INSERT INTO #Events VALUES ('VIEW'), ('CLICK'), ('EXIT');

    CREATE TABLE #Devices (DeviceType VARCHAR(20), OSType VARCHAR(20), BrowserType VARCHAR(20));
    INSERT INTO #Devices VALUES
        ('MOBILE', 'iOS', 'Safari'), ('MOBILE', 'Android', 'Chrome'),
        ('PC', 'Windows', 'Chrome'), ('PAD', 'iOS', 'Safari');

    CREATE TABLE #Referrers (ReferrerType VARCHAR(20), TrafficSource VARCHAR(50));
    INSERT INTO #Referrers VALUES
        ('DIRECT', NULL), ('SEARCH', '百度搜索'), ('AD', '抖音广告'),
        ('SOCIAL', '微信朋友圈'), ('EMAIL', '邮件营销');

    CREATE TABLE #Regions (RegionCode VARCHAR(20), CityName VARCHAR(50));
    INSERT INTO #Regions VALUES
        ('110000', '北京市'), ('310000', '上海市'), ('440100', '广州市'),
        ('440300', '深圳市'), ('330100', '杭州市'), ('320100', '南京市'),
        ('120000', '天津市'), ('510100', '成都市'), ('420100', '武汉市'),
        ('其他', '其他城市');

    -- 5. 插入前优化（需权限）
    EXEC('ALTER INDEX ALL ON [dbo].[traffic_store_path_analysis] DISABLE;');
    EXEC('ALTER DATABASE ' + @DBName + ' SET RECOVERY SIMPLE;');

    -- 6. 循环插入（核心逻辑）
    WHILE @TotalInserted < @RowCount
    BEGIN
        DECLARE @CurrentBatch INT = IIF((@RowCount - @TotalInserted) < @BatchSize,
                                       @RowCount - @TotalInserted, @BatchSize);

        BEGIN TRANSACTION;

        INSERT INTO [dbo].[traffic_store_path_analysis] (
            [log_uuid], [user_id], [session_id], [store_id], [item_id],
            [category_id], [current_page_code], [current_page_name],
            [prev_page_code], [prev_page_name], [event_type],
            [event_action], [access_time], [stay_duration],
            [referrer_type], [traffic_source], [device_type],
            [os_type], [browser_type], [ip_address],
            [region_code], [city_name], [data_quality_flag]
        )
        SELECT
            NEWID(),
            'USER_' + RIGHT('0000000' + CAST(ABS(CHECKSUM(NEWID())) % 1000000 AS VARCHAR(7)), 7),
            'SESS_' + RIGHT('0000000' + CAST(ABS(CHECKSUM(NEWID())) % 1000000 AS VARCHAR(7)), 7),
            'STORE_' + CAST(ABS(CHECKSUM(NEWID())) % 100 AS VARCHAR(3)),
            CASE WHEN p.PageCode = 'ITEM_DETAIL'
                 THEN 'ITEM_' + CAST(ABS(CHECKSUM(NEWID())) % 100000 AS VARCHAR(6))
                 ELSE NULL END,
            CASE WHEN p.PageCode = 'ITEM_LIST'
                 THEN 'CAT_' + CAST(ABS(CHECKSUM(NEWID())) % 1000 AS VARCHAR(4))
                 ELSE NULL END,
            p.PageCode, p.PageName,
            prev_p.PageCode, prev_p.PageName,
            e.EventType,
            CASE e.EventType
                WHEN 'CLICK' THEN
                    CASE p.PageCode
                        WHEN 'HOME' THEN '点击导航栏-' + CASE WHEN ABS(CHECKSUM(NEWID())) % 3 = 0 THEN '商品分类' WHEN ABS(CHECKSUM(NEWID())) % 3 = 1 THEN '营销活动' ELSE '会员入口' END
                        WHEN 'ITEM_LIST' THEN '点击商品卡片-' + CAST(ABS(CHECKSUM(NEWID())) % 1000 AS VARCHAR(4))
                        WHEN 'ITEM_DETAIL' THEN '点击「加入购物车」按钮'
                        WHEN 'CART' THEN '点击「去结算」按钮'
                        WHEN 'CHECKOUT' THEN '点击「提交订单」按钮'
                        ELSE '点击页面按钮-' + CAST(ABS(CHECKSUM(NEWID())) % 100 AS VARCHAR(3))
                    END
                WHEN 'VIEW' THEN '浏览页面内容-' + CASE p.PageCode WHEN 'ITEM_DETAIL' THEN '商品参数|评价' ELSE '页面默认内容' END
                WHEN 'EXIT' THEN '离开页面（停留时长 ' + CAST(CASE WHEN e.EventType = 'EXIT' THEN ABS(CHECKSUM(NEWID())) % 300 ELSE 0 END AS VARCHAR(3)) + ' 秒）'
                ELSE NULL
            END,
            DATEADD(SECOND, -ABS(CHECKSUM(NEWID())) % 2592000, GETDATE()),
            CASE WHEN e.EventType = 'EXIT'
                 THEN ABS(CHECKSUM(NEWID())) % 300
                 ELSE NULL END,
            r.ReferrerType, r.TrafficSource,
            d.DeviceType, d.OSType, d.BrowserType,
            '192.168.' + CAST(ABS(CHECKSUM(NEWID())) % 255 AS VARCHAR(3)) + '.' + CAST(ABS(CHECKSUM(NEWID())) % 255 AS VARCHAR(3)),
            reg.RegionCode, reg.CityName,
            1
        FROM
            -- 替换为兼容低版本的批次数据生成方式
            (SELECT TOP (@CurrentBatch) ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS dummy
             FROM sys.objects o1 CROSS JOIN sys.objects o2) t
            CROSS APPLY (SELECT TOP 1 * FROM #Pages ORDER BY NEWID()) p
            CROSS APPLY (SELECT TOP 1 * FROM #Events ORDER BY NEWID()) e
            CROSS APPLY (SELECT TOP 1 * FROM #Devices ORDER BY NEWID()) d
            CROSS APPLY (SELECT TOP 1 * FROM #Referrers ORDER BY NEWID()) r
            CROSS APPLY (SELECT TOP 1 * FROM #Regions ORDER BY NEWID()) reg
            CROSS APPLY (
                SELECT TOP 1 prev_p.PageCode, prev_p.PageName
                FROM #Pages prev_p
                WHERE prev_p.PageCode <> p.PageCode
                  AND (
                      (p.PageCode = 'HOME' AND prev_p.PageCode IN ('ITEM_LIST', 'ITEM_DETAIL', 'CART', 'MEMBER_CENTER'))
                      OR (p.PageCode = 'ITEM_LIST' AND prev_p.PageCode IN ('HOME', 'ITEM_DETAIL', 'CART'))
                      OR (p.PageCode = 'ITEM_DETAIL' AND prev_p.PageCode IN ('ITEM_LIST', 'CART', 'HOME'))
                      OR (p.PageCode = 'CART' AND prev_p.PageCode IN ('ITEM_DETAIL', 'HOME', 'ITEM_LIST', 'CHECKOUT'))
                      OR (p.PageCode = 'CHECKOUT' AND prev_p.PageCode IN ('CART', 'HOME'))
                      OR (p.PageCode = 'MEMBER_CENTER' AND prev_p.PageCode IN ('HOME', 'ORDER_LIST'))
                      OR (p.PageCode = 'HELP' AND prev_p.PageCode IN ('HOME', 'MEMBER_CENTER'))
                      OR (p.PageCode = 'ORDER_LIST' AND prev_p.PageCode IN ('HOME', 'MEMBER_CENTER'))
                      OR ABS(CHECKSUM(NEWID())) % 5 = 0
                  )
                ORDER BY NEWID()
            ) prev_p;

        COMMIT TRANSACTION;

        SET @TotalInserted += @CurrentBatch;
        PRINT '已完成批次: ' + CAST(@CurrentBatch AS VARCHAR)
              + ', 累计插入: ' + CAST(@TotalInserted AS VARCHAR)
              + ', 剩余: ' + CAST(@RowCount - @TotalInserted AS VARCHAR);
    END;

    -- 7. 恢复环境
    EXEC('ALTER DATABASE ' + @DBName + ' SET RECOVERY FULL;');
    ALTER INDEX ALL ON [dbo].[traffic_store_path_analysis] REBUILD;
END;
GO

-- 执行存储过程
EXEC GenerateTrafficStorePathData @RowCount = 1000000;