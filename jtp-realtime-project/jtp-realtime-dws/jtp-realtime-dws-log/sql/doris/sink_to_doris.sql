INSERT INTO dws_traffic_search_keyword_window_report_doris_sink
SELECT
    substring(CAST(window_start_time AS STRING), 0, 19) AS start_time,
    substring(CAST(window_end_time AS STRING), 0, 19) AS end_time,
    substring(CAST(window_start_time AS STRING), 0, 10) AS cur_date,
    keyword,
    keyword_count
FROM report_table