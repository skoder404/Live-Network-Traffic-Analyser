-- Hourly traffic volume
SELECT event_year, event_month, event_day, event_hour,
       COUNT(*) AS packets, SUM(packet_length) AS bytes
FROM lnta.traffic_raw
GROUP BY event_year, event_month, event_day, event_hour
ORDER BY event_year, event_month, event_day, event_hour;

-- Top destination ports in a selected hour
SELECT dst_port, COUNT(*) AS packets, SUM(packet_length) AS bytes
FROM lnta.traffic_raw
WHERE event_year = 2026 AND event_month = 9 AND event_day = 23 AND event_hour = 10
GROUP BY dst_port
ORDER BY packets DESC
LIMIT 20;