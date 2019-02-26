select
consult_id
,cid
,data_id
,type
,group_id
,is_work_time
,c_first_send_time
,c_last_send_time
,b_first_send_time
,b_last_send_time
,if(b_first_send_time is not null,unix_timestamp(b_first_send_time)-unix_timestamp(c_first_send_time),null) as first_duration
,if(unix_timestamp(b_first_send_time)-unix_timestamp(c_first_send_time)<=60,1,0) as is_60s_resp
,if(unix_timestamp(b_first_send_time)-unix_timestamp(c_first_send_time)<=30,1,0) as is_30s_resp
,if(unix_timestamp(b_last_send_time)>=unix_timestamp(c_last_send_time),1,0) as is_last_resp
,avg_duration
,dt
from (
    select
    consult_id
    ,dt
    ,cid
    ,data_id
    ,type
    ,group_id
    ,first_value(is_work_time) over(partition by consult_id order by customer_send_time asc) as is_work_time
    ,avg(duration) over(partition by consult_id) as avg_duration
    ,min(customer_send_time) over(partition by consult_id) as c_first_send_time
    ,max(customer_send_time) over(partition by consult_id) as c_last_send_time
    ,min(business_send_time) over(partition by consult_id) as b_first_send_time
    ,max(business_send_time) over(partition by consult_id) as b_last_send_time
    from semi_im.im_dialogue        --188180
    where dt >='{dt1}' and dt<='{dt2}'
    and coalesce(duration,0)>=0 --and consult_id is not null--有dialogue上报的bug 同一个dialogue下，b端第一条send_time小于c端的第一条send_time。
    ) temp
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16