-- Completed files have the final .csv suffix; .inprogress-* files are hidden.
CREATE DATABASE IF NOT EXISTS lnta;

CREATE EXTERNAL TABLE IF NOT EXISTS lnta.traffic_raw (
  timestamp STRING, src_ip STRING, dst_ip STRING, src_port INT, dst_port INT,
  protocol STRING, packet_length INT, src_mac STRING, dst_mac STRING,
  tcp_flags STRING, iat_ms DOUBLE
)
PARTITIONED BY (event_year INT, event_month INT, event_day INT, event_hour INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/traffic/raw';

CREATE EXTERNAL TABLE IF NOT EXISTS lnta.traffic_stream (
  timestamp STRING, src_ip STRING, dst_ip STRING, src_port INT, dst_port INT,
  protocol STRING, packet_length INT, src_mac STRING, dst_mac STRING,
  tcp_flags STRING, iat_ms DOUBLE
)
PARTITIONED BY (event_year INT, event_month INT, event_day INT, event_hour INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/traffic/stream_in';

MSCK REPAIR TABLE lnta.traffic_raw;
MSCK REPAIR TABLE lnta.traffic_stream;