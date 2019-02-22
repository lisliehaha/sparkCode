select aa.device_id
      ,case when g.device_id is not null and aa.is_open = 1 then 1 else 0 end as is_highvalue --是否为高价值 0-否 1-是
      ,case when h.open_udid is not null and aa.is_open = 1 then 1 else 0 end as is_first  --是否为首次启动设备 0-否 1-是
      ,aa.deployment_pv
      ,aa.send_pv
      ,aa.feedback_pv
      ,aa.arrive_pv
      ,aa.open_pv
      -- ,f.page_open_pv
 from (select a.open_udid as device_id
           ,e.is_open as is_open
           ,a.dt as dt
           ,count(1) as deployment_pv
           ,count(case when b.msg_id is not null then b.msg_id end) as send_pv
           ,count(case when c.msg_id is not null then c.msg_id end) as feedback_pv
           ,count(case when d.msg_id is not null then d.msg_id end) as arrive_pv
           ,count(case when e.msg_id is not null then e.msg_id end) as open_pv
       from (select dt, open_udid,msg_id from mfw_dwd.fact_flow_server_push_deployment where dt='20190120' ) a
       left join (select dt, msg_id from mfw_dwd.fact_flow_server_push_send where dt='20190120') b
              on a.msg_id = b.msg_id and a.dt=b.dt
       left join (select dt, msg_id from mfw_dwd.fact_flow_server_push_feedback where dt='20190120') c
              on b.msg_id = c.msg_id and b.dt=c.dt
       left join (select dt, msg_id from mfw_dwd.fact_flow_server_push_arrive where dt='20190120') d
              on c.msg_id = d.msg_id and c.dt=d.dt
       left join (select dt, msg_id,1 as is_open from mfw_dwd.fact_flow_server_push_open where dt='20190120') e
              on d.msg_id = e.msg_id and d.dt=e.dt
       group by a.open_udid,e.is_open,a.dt )aa
  left join (select dt,device_id from mfw_dws.aggr_participants_hvuser_dd_day where dt='20190120' group by device_id,dt) g
         on aa.device_id = g.device_id and aa.dt=g.dt
  left join (select dt, open_udid from mfw_dwd.fact_flow_push_first_launch where dt='20190120' group by open_udid,dt) h
         on aa.device_id = h.open_udid and aa.dt=h.dt limit 10