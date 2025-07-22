with cx as (select id,    complex_name,    province_id,    city_id,    district_id,    district_name
            from ods_base_complex
where is_deleted = 0),
     pv as (select id,    name
            from ods_base_region_info
            where is_deleted = 0),
     cy as (select id,    name
            from ods_base_region_info
            where is_deleted = 0),
     ex as (select collect_set(cast(courier_emp_id as string)) courier_emp_id,    complex_id
            from ods_express_courier_complex where
is_deleted = 0
            group by complex_id)
insert overwrite table dim_complex_full partition (dt='2025-07-12')
select cx.id,
       complex_name,
       courier_emp_id,
       province_id,
       pv.name,
       city_id,
       cy.name,
       district_id,
       district_name
from cx
         left join pv    on cx.province_id = pv.id
         left join cy    on cx.city_id = pv.id
         left join ex    on cx.id = ex.complex_id;



with og as (select id,    org_name,    org_level,    region_id,    org_parent_id
            from ods_base_organ where
is_deleted = 0),
     rg as (select id,    name,    dict_code
            from ods_base_region_info where is_deleted = 0)
insert overwrite table dim_organ_full partition(dt = '2025-07-12')
select a.id,
       a.org_name,
       a.org_level,
       a.region_id,
       rg.name,
       dict_code,
       a.org_parent_id,
       pog.org_name
from og a
         left join rg    on a.region_id = rg.id
         left join og pog    on a.org_parent_id = pog.id;



insert overwrite table dim_region_full partition (dt = '2025-07-12')
select id,
       parent_id,
       name,
       dict_code,
       short_name
from ods_base_region_info where is_deleted = 0;


with ex as (
select id,
       emp_id,
       org_id,
       working_phone,
       express_type
    from ods_express_courier where is_deleted=0
),
rg as (
    select
        id,
        org_name
    from ods_base_organ where  is_deleted=0
),dc as (
    select
        id,name
    from ods_base_dic where is_deleted=0
)
insert overwrite table dim_express_courier_full partition (dt='2025-07-12')
select
    ex.id,
emp_id,
org_id,
rg.org_name,
working_phone,
express_type,
dc.name
from ex left join rg
on ex.org_id=rg.id
left join dc
on ex.express_type=dc.id;



with sf as (
    select
        id,
        line_id,
        start_time,
        driver1_emp_id,
        driver2_emp_id,
        truck_id,
        pair_shift_id
    from ods_line_base_shift where is_deleted=0
), le as (
    select
        id,
        name,
        line_no,
        line_level,
        org_id,
        transport_line_type_id,
        start_org_id,
        start_org_name,
        end_org_id,
        end_org_name,
        pair_line_id,
        distance,
        cost,
        estimated_time,
        status
    from ods_line_base_info where is_deleted=0
), bc as (
    select
        id,name
    from ods_base_dic where is_deleted=0
)
insert overwrite table dim_shift_full partition (dt='2025-07-12')
select
sf.id,
line_id,
le.name,
line_no,
line_level,
org_id,
transport_line_type_id,
bc.name,
start_org_id,
start_org_name,
end_org_id,
end_org_name,
pair_line_id,
distance,
cost,
estimated_time,
start_time,
driver1_emp_id,
driver2_emp_id,
truck_id,
pair_shift_id
from sf left join le
on sf.line_id = le.id
left join bc
on le.transport_line_type_id=bc.id;


insert overwrite table tms.dim_truck_driver_full partition (dt = '2025-07-12')
select driver_info.id,
       emp_id,
       org_id,
       organ_info.org_name,
       team_id,
       team_info.name team_name,
       license_type,
       init_license_date,
       expire_date,
       license_no,
       is_enabled
from (select id,
             emp_id,
             org_id,
             team_id,
             license_type,
             init_license_date,
             expire_date,
             license_no,
             is_enabled
      from ods_truck_driver
      where is_deleted = '0') driver_info
         join (
    select id,
           org_name
    from ods_base_organ
    where is_deleted = '0'
) organ_info
              on driver_info.org_id = organ_info.id
         join (
    select id,
           name
    from ods_truck_team
    where is_deleted = '0'
) team_info
on driver_info.team_id = team_info.id;


insert overwrite table tms.dim_truck_full partition (dt = '2025-07-12')
select truck_info.id,
       team_id,
       team_info.name     team_name,
       team_no,
       org_id,
       org_name,
       manager_emp_id,
       truck_no,
       truck_model_id,
       model_name         truck_model_name,
       model_type         truck_model_type,
       dic_for_type.name  truck_model_type_name,
       model_no           truck_model_no,
       brand              truck_brand,
       dic_for_br.name truck_brand_name,
       truck_weight,
       load_weight,
       total_weight,
       eev,
       boxcar_len,
       boxcar_wd,
       boxcar_hg,
       max_speed,
       oil_vol,
       device_gps_id,
       engine_no,
       license_registration_date,
       license_last_check_date,
       license_expire_date,
       is_enabled
from (select id,
             team_id,

             md5(truck_no) truck_no,
             truck_model_id,

             device_gps_id,
             engine_no,
             license_registration_date,
             license_last_check_date,
             license_expire_date,
             is_enabled
      from ods_truck_info
      where is_deleted = '0') truck_info
         join
     (select id,
             name,
             team_no,
             org_id,
             manager_emp_id
      from ods_truck_team
      where is_deleted = '0') team_info
     on truck_info.team_id = team_info.id
         join
     (select id,
             model_name,
             model_type,
             model_no,
             brand,
             truck_weight,
             load_weight,
             total_weight,
             eev,
             boxcar_len,
             boxcar_wd,
             boxcar_hg,
             max_speed,
             oil_vol
      from ods_truck_model
      where is_deleted = '0') model_info
     on truck_info.truck_model_id = model_info.id
         join
     (select id,
             org_name
      from ods_base_organ
      where  is_deleted = '0'
     ) organ_info
     on org_id = organ_info.id
         join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_type
     on model_info.model_type = dic_for_type.id
         join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_br
     on model_info.brand = dic_for_br.id;


-- 8. dim_user_zip
-- 8.1 首日装载
insert overwrite table dim_user_zip partition (dt = '2025-07-12')
select id,
       login_name,
       nick_name,
       md5(passwd)                                                                                    passwd,
       md5(real_name)                                                                                 realname,
       md5(if(phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
              phone_num, null))                                                                       phone_num,
       md5(if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$', email, null)) email,
       user_level,
       date_add('1970-01-01', cast(birthday as int))                                                  birthday,
       gender,
       date_format(from_utc_timestamp(cast(create_time as bigint), 'UTC'),'yyyy-MM-dd')                     start_date,
       '2025-07-12'                                                                                         end_date
from ods_user_info
where is_deleted = '0';

-- 9. dim_user_address_zip
-- 9.1 首日装载
insert overwrite table dim_user_address_zip
    partition (dt = '2025-07-12')
select id,
       user_id,
       md5(if(phone regexp
              '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
              phone, null))               phone,
       province_id,
       city_id,
       district_id,
       complex_id,
       address,
       is_default,
       concat(substr(create_time, 1, 10), ' ',
              substr(create_time, 12, 8)) start_date,
       '2025-07-12'                             end_date
from ods_user_address
where is_deleted = '0';