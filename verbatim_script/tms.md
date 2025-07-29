#------------物流的逐字稿------------
面试官您好！
我最近做过的项目是物流
一、项目背景：为什么要做物流数仓？
“想象一下：一家全国性物流公司，日订单超千万 ，仓库、运输、配送环节产生海量数据 —— 但这些数据分散在不同系统（比如 Oracle 存订单、MySQL 存客户），就像 “信息孤岛”：

痛点 1：数据零散，分析难：想知道 “哪个区域投诉最多”，得跨系统查数据，效率极低；
痛点 2：响应慢，跟不上业务：传统数据库处理千万级数据，报表要跑几小时，无法支持实时决策；
痛点 3：缺乏统一标准：订单、库存、运输的指标各自为战，总部和网点对 “货损率” 定义都不一样！

所以，建数据仓库（数仓）+ 统一指标体系 ，成了必然选择 —— 把分散的数据 “拧成一股绳”，让数据真正成为决策的 “指南针”。”
二、物流数仓怎么建？
数仓搭建是个系统工程，咱们从 技术架构、分层设计、业务主题 三个维度拆解
1. 技术架构：数据从哪来，到哪去？
数据源：通过jar导入mysql的物流信息
采集层:通过python写生成脚本的代码，然后通过运行seatunnel将mysql的数据导入到hive当中
计算层：用 Structured Streaming 做实时 ETL（清洗、转换数据），也支持 Spark 离线计算；
简单说：这套架构像 “数据工厂”，从源头接数据，加工后存到不同 “仓库”，供不同场景使用！
2. 分层设计：数仓的 “金字塔” 逻辑
数仓分三层，层层递进（类比 “洗菜→切菜→炒菜”）：
ODS 层（原始数据层）：
直接 “复制” 数据源的数据，保留最原始的样子（比如订单表的每一条记录）。作用：留底，方便回溯 。
DWD 层（明细数据层）：
对 ODS 数据做 清洗 + 关联 （比如把订单表和客户表关联，补充收货地址、客户等级），生成 “宽表”。作用：让数据更 “好用” 。
DWS 层（服务数据层）：
对 DWD 数据做 指标计算 （比如统计 “全国今日订单量”“华东区货损率”）。作用：直接输出业务可用的指标 。
dwd成的创建了以下表：
小区维度表的字段：
id '小区ID',
complex_name '小区名称',
courier_emp_ids '负责快递员IDS',(数组类型存储了多个快递员的ID)
province_id '省份ID',
province_name '省份名称',
city_id '城市ID',
city_name '城市名称',
district_id '区 / 县ID',
district_name '区 / 县名称'
小区维度表关联的表：
ods_base_complex（）,
ods_base_region_info(),
ods_base_region_info(),
ods_express_courier_complex()
机构维度表：
id '机构ID',
org_name '机构名称',
org_level 机构等级(1为转运中心，2转运站),
region_id 地区ID，1级机构为city，2级机构为district,
region_name '地区名称',
region_code 地区编码（行政级别）,
org_parent_id '父级机构ID',
org_parent_mame '父级机构名称',
机构维度表关联的表：
ods_base_organ(),
ods_base_region_info()
地区维度表:
id  '地区ID',
parent_id   '上级地区ID',
name    '地区名称',
dict_code   编码（行政级别）,
short_name  "简称",
地区维度表关联的表：
ods_base_organ(),
ods_base_region_info()
快递员维度表:
id  '快递员ID',
emp_id  '员工ID',
org_id  '所属机构ID',
org_name    '机构名称',
working_phone   '工作电话',
express_type    快递员类型(收货;发货),
express_type_name   '快递员类型名称',
快递员维度表关联的表：
ods_express_courier(),
ods_base_organ(),
ods_base_dic()
班次维度表:
id  班次ID,
line_id 线路ID,
line_name   线路名称,
line_no 线路编号,
line_level  线路级别,
org_id  所属机构,
transport_line_type_id  线路类型ID,
transport_line_type_name    线路类型名称,
start_org_id    起始机构ID,
start_org_name  起始机构名称,
end_org_id  目标机构ID,
end_org_name    目标机构名称,
pair_line_id    配对线路ID,
distance    直线距离,
cost    公路里程,
estimated_time  分钟,
start_time  班次开始时间,
driver1_emp_id  第一司机,
driver2_emp_id  第二司机,
truck_id    卡车ID,
pair_shift_id   同一辆车一去一回的另一班次,
班次维度表关联的表：
ods_line_base_shift(),
ods_line_base_info(),
ods_base_dic()
司机维度表:
id '司机信息ID',
emp_id '员工ID',
org_id '所属机构ID',
org_name '所属机构名称',
team_id '所属车队ID',
tream_name '所属车队名称',
license_type '准驾车型',
init_license_date '初次领证日期',
expire_date '有效截止日期',
license_no '驾驶证号',
is_enabled 状态 0：禁用 1：正常,
司机维度表关联的表：
ods_truck_driver(),
ods_base_organ(),
ods_truck_team()
卡车维度表:
id '卡车ID'
team_id '所属车队ID'
team_name '所属车队名称'
team_no '车队编号'
org_id '所属机构'
org_name '所属机构名称'
manager_emp_id '负责人'
truck_no '车牌号码'
truck_model_id '型号'
truck_model_name '型号名称'
truck_model_type '型号类型'
卡车维度表关联的表：
ods_truck_info(),
ods_truck_team(),
ods_truck_model(),
ods_base_organ(),
ods_base_dic(),
ods_base_dic()
用户拉链表:
id 用户地址信息ID,
login_name 用户名称,
nick_name 用户昵称,
passwd 用户密码,
real_name 用户姓名,
phone_num 手机号,
email 邮箱,
user_level 用户级别,
birthday 用户生日,
gender 性别 M男,F女,
start_date 起始日期,
用户拉链表关联的表：
ods_user_info()
用户地址拉链表:
id  地址ID
user_id 用户ID
phone   电话号
province_id 所属省份ID
city_id 所属城市ID
district_id 所属区县ID
complex_id  所属小区ID
address 详细地址
is_default  是否默认
start_date  起始日期
end_date    结束日期
用户地址拉链表关联的表：
ods_user_address()
dwd成的创建了以下表：
交易域订单明细事务事实表:
id  '运单明细ID'
order_id    '运单ID'
cargo_type  '货物类型ID'
cargo_type_name '货物类型名称'
volumn_length   '长cm'
volumn_width    '宽cm'
volumn_height   '高cm'
weight  重量 kg
order_time  '下单时间'
order_no    '运单号'
status  '运单状态'
status_name '运单状态名称'
collect_type    取件类型，1为网点自寄，2为上门取件
collect_type_name   '取件类型名称'
user_id '用户ID'
receiver_complex_id '收件人小区id'
receiver_province_id    '收件人省份id'
receiver_city_id    '收件人城市id'
receiver_district_id    '收件人区县id'
receiver_name   '收件人姓名'
sender_complex_id   '发件人小区id'
sender_province_id  '发件人省份id'
sender_city_id  '发件人城市id'
sender_district_id  '发件人区县id'
sender_name '发件人姓名'
cargo_num   '货物个数'
amount  '金额'
estimate_arrive_time    '预计到达时间'
distance    距离，单位：公里
ts  '时间戳'
交易域订单明细事务事实表的关联表：
ods_order_cargo(),
ods_order_info(),
ods_base_dic(),
ods_base_dic(),
ods_base_dic()
交易域支付成功事务事实表:
id  '运单明细ID',
order_id    '运单ID',
cargo_type  '货物类型ID',
cargo_type_name '货物类型名称',
volumn_length   '长cm',
volumn_width    '宽cm',
volumn_height   '高cm',
weight  重量 kg,
payment_time    '支付时间',
order_no    '运单号',
status  '运单状态',
status_name '运单状态名称',
collect_type    取件类型，1为网点自寄，2为上门取件,
collect_type_name   '取件类型名称',
user_id '用户ID',
receiver_complex_id '收件人小区id',
receiver_province_id    '收件人省份id',
receiver_city_id    '收件人城市id',
receiver_district_id    '收件人区县id',
receiver_name   '收件人姓名',
sender_complex_id   '发件人小区id',
sender_province_id  '发件人省份id',
sender_city_id  '发件人城市id',
sender_district_id  '发件人区县id',
sender_name '发件人姓名',
payment_type    '支付方式',
payment_type_name   '支付方式名称',
cargo_num   '货物个数',
amount  '金额',
estimate_arrive_time    '预计到达时间',
distance    距离，单位：公里,
ts  '时间戳',
交易域支付成功事务事实表的关联表：
ods_order_cargo(),
ods_order_info(),
ods_base_dic(),
ods_base_dic(),
ods_base_dic(),
ods_base_dic()
交易域取消运单事务事实表:
id  '运单明细ID',
order_id    '运单ID',
cargo_type  '货物类型ID',
cargo_type_name '货物类型名称',
volumn_length   '长cm',
volumn_width    '宽cm',
volumn_height   '高cm',
weight  重量 kg,
cancel_time '取消时间',
order_no    '运单号',
status  '运单状态',
status_name '运单状态名称',
collect_type    取件类型，1为网点自寄，2为上门取件,
collect_type_name   '取件类型名称',
user_id '用户ID',
receiver_complex_id '收件人小区id',
receiver_province_id    '收件人省份id',
receiver_city_id    '收件人城市id',
receiver_district_id    '收件人区县id',
receiver_name   '收件人姓名',
sender_complex_id   '发件人小区id',
sender_province_id  '发件人省份id',
sender_city_id  '发件人城市id',
sender_district_id  '发件人区县id',
sender_name '发件人姓名',
cargo_num   '货物个数',
amount  '金额',
estimate_arrive_time    '预计到达时间',
distance    距离，单位：公里,
ts  '时间戳',
交易域取消运单事务事实表的关联表：
ods_order_cargo(),
ods_order_info(),
ods_base_dic(),
ods_base_dic(),
ods_base_dic()
物流域揽收事务事实表:
id  '运单明细ID',
order_id    '运单ID',
cargo_type  '货物类型ID',
cargo_type_name '货物类型名称',
volumn_length   '长cm',
volumn_width    '宽cm',
volumn_height   '高cm',
weight  重量 kg,
receive_time    '揽收时间',
order_no    '运单号',
status  '运单状态',
status_name '运单状态名称',
collect_type    取件类型，1为网点自寄，2为上门取件,
collect_type_name   '取件类型名称',
user_id '用户ID',
receiver_complex_id '收件人小区id',
receiver_province_id    '收件人省份id',
receiver_city_id    '收件人城市id',
receiver_district_id    '收件人区县id',
receiver_name   '收件人姓名',
sender_complex_id   '发件人小区id',
sender_province_id  '发件人省份id',
sender_city_id  '发件人城市id',
sender_district_id  '发件人区县id',
sender_name '发件人姓名',
payment_type    '支付方式',
payment_type_name   '支付方式名称',
cargo_num   '货物个数',
amount  '金额',
estimate_arrive_time    '预计到达时间',
distance    距离，单位：公里,
ts  '时间戳',
物流域揽收事务事实表的关联表：
ods_order_cargo(),
ods_order_info(),
ods_base_dic(),
ods_base_dic(),
ods_base_dic(),
ods_base_dic()
物流域发单事务事实表:
id  '运单明细ID',
order_id    '运单ID',
cargo_type  '货物类型ID',
cargo_type_name '货物类型名称',
volumn_length   '长cm',
volumn_width    '宽cm',
volumn_height   '高cm',
weight  重量 kg,
dispatch_time   '发单时间',
order_no    '运单号',
status  '运单状态',
status_name '运单状态名称',
collect_type    取件类型，1为网点自寄，2为上门取件,
collect_type_name   '取件类型名称',
user_id '用户ID',
receiver_complex_id '收件人小区id',
receiver_province_id    '收件人省份id',
receiver_city_id    '收件人城市id',
receiver_district_id    '收件人区县id',
receiver_name   '收件人姓名',
sender_complex_id   '发件人小区id',
sender_province_id  '发件人省份id',
sender_city_id  '发件人城市id',
sender_district_id  '发件人区县id',
sender_name '发件人姓名',
payment_type    '支付方式',
payment_type_name   '支付方式名称',
cargo_num   '货物个数',
amount  '金额',
estimate_arrive_time    '预计到达时间',
distance    距离，单位：公里,
ts  '时间戳',
物流域发单事务事实表的关联表：
ods_order_cargo(),
ods_order_info(),
ods_base_dic(),
ods_base_dic(),
ods_base_dic(),
ods_base_dic()
物流域转运完成事务事实表:
id  '运单明细ID'，
order_id    '运单ID'，
cargo_type  '货物类型ID'，
cargo_type_name '货物类型名称'，
volumn_length   '长cm'，
volumn_width    '宽cm'，
volumn_height   '高cm'，
weight  重量 kg，
bound_finish_time   '转运完成时间'，
order_no    '运单号'，
status  '运单状态'，
status_name '运单状态名称'，
collect_type    取件类型，1为网点自寄，2为上门取件，
collect_type_name   '取件类型名称'，
user_id '用户ID'，
receiver_complex_id '收件人小区id'，
receiver_province_id    '收件人省份id'，
receiver_city_id    '收件人城市id'，
receiver_district_id    '收件人区县id'，
receiver_name   '收件人姓名'，
sender_complex_id   '发件人小区id'，
sender_province_id  '发件人省份id'，
sender_city_id  '发件人城市id'，
sender_district_id  '发件人区县id'，
sender_name '发件人姓名'，
payment_type    '支付方式'，
payment_type_name   '支付方式名称'，
cargo_num   '货物个数'，
amount  '金额'，
estimate_arrive_time    '预计到达时间'，
distance    距离，单位：公里，
ts  '时间戳'，
物流域转运完成事务事实表的关联表：
ods_order_cargo(),
ods_order_info(),
ods_base_dic(),
ods_base_dic(),
ods_base_dic(),
ods_base_dic()
物流域派送成功事务事实表:
id  '运单明细ID',
order_id    '运单ID',
cargo_type  '货物类型ID',
cargo_type_name '货物类型名称',
volumn_length   '长cm',
volumn_width    '宽cm',
volumn_height   '高cm',
weight  重量 kg,
deliver_suc_time    '派送成功时间',
order_no    '运单号',
status  '运单状态',
status_name '运单状态名称',
collect_type    取件类型，1为网点自寄，2为上门取件,
collect_type_name   '取件类型名称',
user_id '用户ID',
receiver_complex_id '收件人小区id',
receiver_province_id    '收件人省份id',
receiver_city_id    '收件人城市id',
receiver_district_id    '收件人区县id',
receiver_name   '收件人姓名',
sender_complex_id   '发件人小区id',
sender_province_id  '发件人省份id',
sender_city_id  '发件人城市id',
sender_district_id  '发件人区县id',
sender_name '发件人姓名',
payment_type    '支付方式',
payment_type_name   '支付方式名称',
cargo_num   '货物个数',
amount  '金额',
estimate_arrive_time    '预计到达时间',
distance    距离，单位：公里,
ts  '时间戳',
物流域派送成功事务事实表的关联表：
ods_order_cargo(),
ods_order_info(),
ods_base_dic(),
ods_base_dic(),
ods_base_dic(),
ods_base_dic()
物流域签收事务事实表:
id  '运单明细ID',
order_id    '运单ID',
cargo_type  '货物类型ID',
cargo_type_name '货物类型名称',
volumn_length   '长cm',
volumn_width    '宽cm',
volumn_height   '高cm',
weight  重量 kg,
sign_time   '签收时间',
order_no    '运单号',
status  '运单状态',
status_name '运单状态名称',
collect_type    取件类型，1为网点自寄，2为上门取件,
collect_type_name   '取件类型名称',
user_id '用户ID',
receiver_complex_id '收件人小区id',
receiver_province_id    '收件人省份id',
receiver_city_id    '收件人城市id',
receiver_district_id    '收件人区县id',
receiver_name   '收件人姓名',
sender_complex_id   '发件人小区id',
sender_province_id  '发件人省份id',
sender_city_id  '发件人城市id',
sender_district_id  '发件人区县id',
sender_name '发件人姓名',
payment_type    '支付方式',
payment_type_name   '支付方式名称',
cargo_num   '货物个数',
amount  '金额',
estimate_arrive_time    '预计到达时间',
distance    距离，单位：公里,
ts  '时间戳',
物流域签收事务事实表的关联表：
ods_order_cargo(),
ods_order_info(),
ods_base_dic(),
ods_base_dic(),
ods_base_dic(),
ods_base_dic()
交易域运单累积快照事实表:
id  '运单明细ID',
order_id    '运单ID',
cargo_type  '货物类型ID',
cargo_type_name '货物类型名称',
volumn_length   '长cm',
volumn_width    '宽cm',
volumn_height   '高cm',
weight  重量 kg,
order_time  '下单时间',
order_no    '运单号',
status  '运单状态',
status_name '运单状态名称',
collect_type    取件类型，1为网点自寄，2为上门取件,
collect_type_name   '取件类型名称',
user_id '用户ID',
receiver_complex_id '收件人小区id',
receiver_province_id    '收件人省份id',
receiver_city_id    '收件人城市id',
receiver_district_id    '收件人区县id',
receiver_name   '收件人姓名',
sender_complex_id   '发件人小区id',
sender_province_id  '发件人省份id',
sender_city_id  '发件人城市id',
sender_district_id  '发件人区县id',
sender_name '发件人姓名',
payment_type    '支付方式',
payment_type_name   '支付方式名称',
cargo_num   '货物个数',
amount  '金额',
estimate_arrive_time    '预计到达时间',
distance    距离，单位：公里,
ts  '时间戳',
start_date  '开始日期',
end_date    '结束日期',
交易域运单累积快照事实表的关联表：
ods_order_cargo(),
ods_order_info(),
ods_base_dic(),
ods_base_dic(),
ods_base_dic(),
ods_base_dic()
物流域运输事务事实表:
id  '运输任务ID',
shift_id    '车次ID',
line_id '路线ID',
start_org_id    '起始机构ID',
start_org_name  '起始机构名称',
end_org_id  '目的机构ID',
end_org_name    '目的机构名称',
order_num   '运单个数',
driver1_emp_id  '司机1ID',
driver1_name    '司机1名称',
driver2_emp_id  '司机2ID',
driver2_name    '司机2名称',
truck_id    '卡车ID',
truck_no    '卡车号牌',
actual_start_time   '实际启动时间',
actual_end_time '实际到达时间',
estimate_end_time   '预估到达时间',
actual_distance '实际行驶距离',
finish_dur_sec  运输完成历经时长：秒,
ts  '时间戳',
物流域运输事务事实表的关联表：
ods_transport_task(),
dim_shift_full()

中转域入库事务事实表:
id  中转记录ID,
order_id    运单ID,
org_id  机构ID,
inbound_time    入库时间,
inbound_emp_id  入库人员,
中转域入库事务事实表的关联表：
ods_order_org_bound()

中转域分拣事务事实表:
id  中转记录ID,
order_id    运单ID,
org_id  机构ID,
inbound_time    入库时间,
inbound_emp_id  入库人员,
中转域分拣事务事实表的关联表：
ods_order_org_bound()

中转域出库事务事实表:
id  中转记录ID,
order_id    订单ID,
org_id  机构ID,
outbound_time   出库时间,
outbound_emp_id 出库人员,
中转域出库事务事实表的关联表：
ods_order_org_bound()
dws成的创建了以下表：
-- 最近 1/7/30 日下单数
-- 最近 1/7/30 日下单金额
-- 最近 1/7/30 日各类型货物下单数
-- 最近 1/7/30 日各类型货物下单金额
-- 最近 1/7/30 日各城市下单数
-- 最近 1/7/30 日各城市下单金额
-- 最近 1/7/30 日各机构下单数
-- 最近 1/7/30 日各机构下单金额
-- 被依赖的
-- 最近 1/7/30 日各机构各类型货物下单数
-- 最近 1/7/30 日各机构各类型货物下单金额
交易域机构货物类型粒度下单:
org_id  机构ID,
org_name    转运站名称,
city_id 城市ID,
city_name   城市名称,
cargo_type  货物类型,
cargo_type_name 货物类型名称,
order_count 下单数,
order_amount    下单金额,
交易域机构货物类型粒度下单关联表：
dwd_trade_order_detail_inc(),
dim_organ_full()
交易域机构货物类型粒度下单:
org_id  机构ID,
org_name    转运站名称,
city_id 城市ID,
city_name   城市名称,
cargo_type  货物类型,
cargo_type_name 货物类型名称,
order_count 下单数,
order_amount    下单金额,
交易域机构货物类型粒度下单关联表：
dwd_trade_order_detail_inc(),
dim_organ_full()
交易域机构货物类型粒度下单 n 日汇总表:
org_id  机构ID,
org_name    转运站名称,
city_id 城市ID,
city_name   城市名称,
cargo_type  货物类型,
cargo_type_name 货物类型名称,
recent_days 最近天数,
order_count 下单数,
order_amount    下单金额,
交易域机构货物类型粒度下单 n 日汇总表的关联表：
dws_trade_org_cargo_type_order_1d()

-- 最近 1/7/30 日接单总数
-- 最近 1/7/30 日接单金额
-- 最近 1/7/30 日各省份揽收次数
-- 最近 1/7/30 日各省份揽收金额
-- 最近 1/7/30 日各城市揽收次数
-- 最近 1/7/30 日各城市揽收金额
-- 最近 1/7/30 日各转运站揽收次数
-- 最近 1/7/30 日各转运站揽收金额
-- 被依赖的
-- 最近 1/7/30 日各转运站揽收次数
交易域机构货物类型粒度下单 n 日汇总表:
org_id  转运站ID,
org_name    转运站名称,
city_id 城市ID,
city_name   城市名称,
province_id 省份ID,
province_name   省份名称,
order_count 揽收次数,
order_amount    揽收金额,
交易域机构货物类型粒度下单 n 日汇总表的关联表：
dwd_trans_receive_detail_inc(),
dim_organ_full(),
dim_region_full(),
dim_region_full(),
dim_region_full()
物流域转运站粒度揽收 n 日汇总表:
org_id  转运站ID,
org_name    转运站名称,
city_id 城市ID,
city_name   城市名称,
province_id 省份ID,
province_name   省份名称,
recent_days 最近天数,
order_count 揽收次数,
order_amount    揽收金额,
物流域转运站粒度揽收 n 日汇总表关联表：
dws_trans_org_receive_1d
物流域发单 1 日汇总表:
order_count 发单总数，
order_amount 发单总金额，
物流域发单 1 日汇总表关联表：
dwd_trans_dispatch_detail_inc()
物流域发单 1 日汇总表:
recent_days 最近天数,
order_count 发单总数,
order_amount    发单总金额,
物流域发单 1 日汇总表关联表：
dws_trans_dispatch_1d()
-- 历史至今运输中运单总数
-- 历史至今运输中运单总金额
-- 被依赖的
-- 历史至今发单数
-- 历史至今发单金额
物流域发单历史至今汇总表:
order_count 发单数,
order_amount    发单金额,
物流域发单历史至今汇总表关联表：
dws_trans_dispatch_1d(),
dws_trans_dispatch_td(),
物流域转运完成历史至今汇总表:
order_count 发单数,
order_amount    发单金额,
物流域转运完成历史至今汇总表关联表：
dwd_trans_bound_finish_detail_inc()
物流域转运完成历史至今汇总表:
order_count 发单数,
order_amount    发单金额,
物流域转运完成历史至今汇总表关联表：
dwd_trans_bound_finish_detail_inc()
-- 最近 1 日完成运输次数
-- 最近 1 日完成运输里程
-- 最近 1 日完成运输时长
-- 最近 1 日各城市完成运输次数
-- 最近 1 日各城市完成运输里程
-- 最近 1 日各城市完成运输时长
-- 最近 1 日各机构完成运输次数
-- 最近 1 日各机构完成运输里程
-- 最近 1 日各机构完成运输时长
-- 最近 1 日各类卡车完成运输次数
-- 最近 1 日各类卡车完成运输里程
-- 最近 1 日各类卡车完成运输时长
-- 被依赖的
-- 最近 1 日各机构各类卡车完成运输次数
-- 最近 1 日各机构各类卡车完成运输里程
-- 最近 1 日各机构各类卡车完成运输时长
物流域机构卡车类别粒度运输最近:
org_id  ‘机构ID
org_name    '机构名称'
truck_model_type    '卡车类别编码'
truck_model_type_name   '卡车类别名称'
trans_finish_count  '运输完成次数'
trans_finish_distance   '运输完成里程'
trans_finish_dur_sec    运输完成时长，单位：秒
物流域机构卡车类别粒度运输最近关联表:
dwd_trans_trans_finish_inc(),
dim_truck_full()