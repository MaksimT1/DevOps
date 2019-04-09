-- Last Hour Bad
select key, 
timestamp(hbase_timestamp), 
unix_timestamp(timestamp(hbase_timestamp), 'yyyy-MM-dd HH:mm:ss'), 
unix_timestamp(serial, 'yyyyMMddHHmmss'), 
unix_timestamp(timestamp(hbase_timestamp), 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(serial, 'yyyyMMddHHmmss') 
from ea_sc_kif.sc_order_details
where unix_timestamp(timestamp(hbase_timestamp), 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(serial, 'yyyyMMddHHmmss') > 300 
and timestamp(hbase_timestamp) > (unix_timestamp(CURRENT_TIMESTAMP) - 3600)
