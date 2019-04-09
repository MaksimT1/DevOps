-- Latest Good
select key, timestamp(hbase_timestamp), unix_timestamp(timestamp(hbase_timestamp), 'yyyy-MM-dd HH:mm:ss'), unix_timestamp(serial, 'yyyyMMddHHmmss'), unix_timestamp(timestamp(hbase_timestamp), 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(serial, 'yyyyMMddHHmmss') from ea_sc_kif.sc_order_details
where unix_timestamp(timestamp(hbase_timestamp), 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(serial, 'yyyyMMddHHmmss') < 300 order by hbase_timestamp desc limit 1000
 
 
-- Last Hour Bad
select key, timestamp(hbase_timestamp), unix_timestamp(timestamp(hbase_timestamp), 'yyyy-MM-dd HH:mm:ss'), unix_timestamp(serial, 'yyyyMMddHHmmss'), unix_timestamp(timestamp(hbase_timestamp), 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(serial, 'yyyyMMddHHmmss') from ea_sc_kif.sc_order_details
where unix_timestamp(timestamp(hbase_timestamp), 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(serial, 'yyyyMMddHHmmss') > 300 and timestamp(hbase_timestamp) > (unix_timestamp(CURRENT_TIMESTAMP) - 3600)
 
 
 
-- Count Bad
select count(*) --key, timestamp(hbase_timestamp), unix_timestamp(timestamp(hbase_timestamp), 'yyyy-MM-dd HH:mm:ss'), unix_timestamp(serial, 'yyyyMMddHHmmss'), unix_timestamp(timestamp(hbase_timestamp), 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(serial, 'yyyyMMddHHmmss')
from ea_sc_kif.sc_order_details
where unix_timestamp(timestamp(hbase_timestamp), 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(serial, 'yyyyMMddHHmmss') > 300 and timestamp(hbase_timestamp) > (unix_timestamp(CURRENT_TIMESTAMP) - 3600)
 
select unix_timestamp(CURRENT_TIMESTAMP)
 
-- Latest ALL
select key, timestamp(hbase_timestamp), unix_timestamp(timestamp(hbase_timestamp), 'yyyy-MM-dd HH:mm:ss'), unix_timestamp(serial, 'yyyyMMddHHmmss'), unix_timestamp(timestamp(hbase_timestamp), 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(serial, 'yyyyMMddHHmmss') from ea_sc_kif.sc_order_details
--where unix_timestamp(timestamp(hbase_timestamp), 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(serial, 'yyyyMMddHHmmss') < 300
order by hbase_timestamp desc  limit 1000
 
-- Alerting view
create view tst_order_detail_large_gap_last_hour
as
select count(*) 
from ea_sc_kif.sc_order_details
where unix_timestamp(timestamp(hbase_timestamp), 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(serial, 'yyyyMMddHHmmss') > 300 and timestamp(hbase_timestamp) > (unix_timestamp(CURRENT_TIMESTAMP) - 3600);
 
 
select * from tst_order_detail_large_gap_last_hour
