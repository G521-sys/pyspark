-- 1 dwd_trade_order_detail_inc
-- 11 首日装载
set hiveexecdynamicpartitionmode=nonstrict;
insert overwrite table tms.dwd_trade_order_detail_inc partition (dt='2025-07-12')
select cargo.id,
       cargo.order_id,
       cargo.cargo_type,
       dic_for_cargo_type.name               cargo_type_name,
       cargo.volume_length,
       cargo.volume_width,
       cargo.volume_height,
       cargo.weight,
       cargo.order_time,
       info.order_no,
       info.status,
       dic_for_status.name                   status_name,
       info.collect_type,
       dic_for_collect_type.name             collect_type_name,
       info.user_id,
       info.receiver_complex_id,
       info.receiver_province_id,
       info.receiver_city_id,
       info.receiver_district_id,
       info.receiver_name,
       info.sender_complex_id,
       info.sender_province_id,
       info.sender_city_id,
       info.sender_district_id,
       info.sender_name,
       info.cargo_num,
       info.amount,
       info.estimate_arrive_time,
       info.distance,
       cargo.ds
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
             concat(substr(create_time, 1, 10), ' ', substr(create_time, 12, 8)) order_time,
             ds
      from ods_order_cargo
      where is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*') receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')   sender_name,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
             distance
      from ods_order_info
      where is_deleted = '0') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string);

-- 2 dwd_trade_pay_suc_detail_inc
-- 21 首日装载
set hiveexecdynamicpartitionmode=nonstrict;
insert overwrite table tms.dwd_trade_pay_suc_detail_inc partition (dt='2025-07-12')
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       payment_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(payment_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
             ds
      from ods_order_cargo
      where is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) payment_time
      from ods_order_info
      where is_deleted = '0'
        and status <> '60010'
        and status <> '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 3 dwd_trade_order_cancel_detail_inc
-- 31 首日装载
set hiveexecdynamicpartitionmode=nonstrict;
insert overwrite table tms.dwd_trade_order_cancel_detail_inc partition (dt='2025-07-12')
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       cancel_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(cancel_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight
      from ods_order_cargo
      where is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) cancel_time
      from ods_order_info
      where is_deleted = '0') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string);
-- 4 dwd_trans_receive_detail_inc
-- 41 首日装载
set hiveexecdynamicpartitionmode=nonstrict;
insert overwrite table tms.dwd_trans_receive_detail_inc
    partition (dt='2025-07-12')
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       receive_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(receive_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight
      from ods_order_cargo
      where is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) receive_time
      from ods_order_info
      where is_deleted = '0') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 5 dwd_trans_dispatch_detail_inc
-- 51 首日装载
set hiveexecdynamicpartitionmode=nonstrict;
insert overwrite table tms.dwd_trans_dispatch_detail_inc partition (dt='2025-07-12')
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       dispatch_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(dispatch_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight
      from ods_order_cargo
      where is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) dispatch_time
      from ods_order_info
      where is_deleted = '0'
        and status <> '60010'
        and status <> '60020'
        and status <> '60030'
        and status <> '60040'
        and status <> '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);
-- 6 dwd_trans_bound_finish_detail_inc
-- 61 首日装载
set hiveexecdynamicpartitionmode=nonstrict;
insert overwrite table tms.dwd_trans_bound_finish_detail_inc
    partition (dt='2025-07-12')
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       bound_finish_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(bound_finish_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight
      from ods_order_cargo
      where is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) bound_finish_time
      from ods_order_info
      where is_deleted = '0') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);


-- 7 dwd_trans_deliver_suc_detail_inc
-- 71 首日装载
set hiveexecdynamicpartitionmode=nonstrict;
insert overwrite table tms.dwd_trans_deliver_suc_detail_inc
    partition (dt='2025-07-12')
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       deliver_suc_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(deliver_suc_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight
      from ods_order_cargo
      where is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) deliver_suc_time
      from ods_order_info
      where is_deleted = '0') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 72 每日装载
with deliver_suc_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 deliver_suc_time
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') deliver_suc_time
                from ods_order_info
                where status = '60070'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volumn_length,
                volumn_width,
                volumn_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '9999-12-31'
           and (status = '60010' or
                status = '60020' or
                status = '60030' or
                status = '60040' or
                status = '60050' or
                status = '60060')
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
                      ds
               from ods_order_cargo) cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info) info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table tms.dwd_trans_deliver_suc_detail_inc
    partition(dt = '2025-07-12')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volumn_length,
       volumn_width,
       volumn_height,
       weight,
       deliver_suc_info.deliver_suc_time,
       order_info.order_no,
       deliver_suc_info.status,
       deliver_suc_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       deliver_suc_info.payment_type,
       deliver_suc_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance,
       ''
from deliver_suc_info
         join order_info
              on deliver_suc_info.id = order_info.order_id;

-- 8 dwd_trans_sign_detail_inc
-- 81 首日装载
set hiveexecdynamicpartitionmode=nonstrict;
insert overwrite table tms.dwd_trans_sign_detail_inc
    partition (dt='2025-07-12')
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       sign_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       ds
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
             ds
      from ods_order_cargo
      where is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) sign_time
      from ods_order_info
      where is_deleted = '0') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic where is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);
-- 9 dwd_trade_order_process_inc
set hiveexecdynamicpartitionmode=nonstrict;
insert overwrite table tms.dwd_trade_order_process_inc
    partition (dt='2025-07-12')
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name               cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       order_time,
       order_no,
       status,
       dic_for_status.name                   status_name,
       collect_type,
       dic_for_collect_type.name             collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name             payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(order_time, 'yyyy-MM-dd') start_date,
       end_date,
       end_date                              dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
             concat(substr(create_time, 1, 10), ' ', substr(create_time, 12, 8)) order_time,
             ds
      from ods_order_cargo
      where is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*') receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')   sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
             distance,
             if(status = '60080' or
                status = '60999',
                concat(substr(update_time, 1, 10)),
                '9999-12-31')                               end_date
      from ods_order_info
      where is_deleted = '0') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);


-- 10 dwd_trans_trans_finish_inc
-- 101 首日装载
set hiveexecdynamicpartitionmode=nonstrict;
insert overwrite table dwd_trans_trans_finish_inc
    partition (dt='2025-07-12')
select info.id,
       shift_id,
       line_id,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       order_num,
       driver1_emp_id,
       driver1_name,
       driver2_emp_id,
       driver2_name,
       truck_id,
       truck_no,
       actual_start_time,
       actual_end_time,
       estimated_time estimate_end_time,
       actual_distance,
       finish_dur_sec,
       dt
from (select id,
             shift_id,
             line_id,
             start_org_id,
             start_org_name,
             end_org_id,
             end_org_name,
             order_num,
             driver1_emp_id,
             concat(substr(driver1_name, 1, 1), '*')                                            driver1_name,
             driver2_emp_id,
             concat(substr(driver2_name, 1, 1), '*')                                            driver2_name,
             truck_id,
             md5(truck_no)                                                                      truck_no,
             date_format(from_utc_timestamp(
                                 cast(actual_start_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                                       actual_start_time,
             date_format(from_utc_timestamp(
                                 cast(actual_end_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                                       actual_end_time,

             actual_distance,
             (cast(actual_end_time as bigint) - cast(actual_start_time as bigint)) / 1000 finish_dur_sec,
             ds,
             date_format(from_utc_timestamp(
                                 cast(actual_end_time as bigint), 'UTC'),
                         'yyyy-MM-dd')                                                                dt
      from ods_transport_task
      where is_deleted = '0') info
         left join
     (select id,
             estimated_time
      from dim_shift_full) dim_tb
     on info.shift_id = dim_tb.id;

-- 102 每日装载
insert overwrite table dwd_trans_trans_finish_inc
    partition (dt = '2025-07-12')
select info.id,
       shift_id,
       line_id,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       order_num,
       driver1_emp_id,
       driver1_name,
       driver2_emp_id,
       driver2_name,
       truck_id,
       truck_no,
       actual_start_time,
       actual_end_time,
       estimated_time estimate_end_time,
       actual_distance,
       finish_dur_sec,
       ts
from (select id,
             shift_id,
             line_id,
             start_org_id,
             start_org_name,
             end_org_id,
             end_org_name,
             order_num,
             driver1_emp_id,
             concat(substr(driver1_name, 1, 1), '*')                                            driver1_name,
             driver2_emp_id,
             concat(substr(driver2_name, 1, 1), '*')                                            driver2_name,
             truck_id,
             md5(truck_no)                                                                      truck_no,
             date_format(from_utc_timestamp(
                                 cast(actual_start_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                                       actual_start_time,
             date_format(from_utc_timestamp(
                                 cast(actual_end_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                                       actual_end_time,
             actual_distance,
             (cast(actual_end_time as bigint) - cast(actual_start_time as bigint)) / 1000 finish_dur_sec,
             ds                                                                                       ts
      from ods_transport_task
      where actual_end_time is not null
        and is_deleted = '0') info
         left join
     (select id,
             estimated_time
      from dim_shift_full) dim_tb
     on info.shift_id = dim_tb.id;

-- 11 dwd_bound_inbound_inc
-- 111 首日装载
set hiveexecdynamicpartitionmode=nonstrict;
insert overwrite table dwd_bound_inbound_inc
    partition (dt='2025-07-12')
select id,
       order_id,
       org_id,
       date_format(from_utc_timestamp(
                           cast(inbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss') inbound_time,
       inbound_emp_id
from ods_order_org_bound;

-- 12 dwd_bound_sort_inc
-- 121 首日装载
set hiveexecdynamicpartitionmode=nonstrict;
insert overwrite table dwd_bound_sort_inc
    partition (dt='2025-07-12')
select id,
       order_id,
       org_id,
       date_format(from_utc_timestamp(
                           cast(sort_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss') sort_time,
       sorter_emp_id
from ods_order_org_bound;

-- 122 每日装载
insert overwrite table dwd_bound_sort_inc
    partition (dt = '2025-07-12')
select id,
       order_id,
       org_id,
       date_format(from_utc_timestamp(
                           cast(sort_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss') sort_time,
       sorter_emp_id
from ods_order_org_bound
where sort_time is not null
  and is_deleted = '0';

-- 13 dwd_bound_outbound_inc
-- 131 首日装载
set hiveexecdynamicpartitionmode=nonstrict;
insert overwrite table dwd_bound_outbound_inc
    partition (dt='2025-07-12')
select id,
       order_id,
       org_id,
       '',
       outbound_emp_id
from ods_order_org_bound;