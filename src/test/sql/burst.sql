DROP TABLE IF EXISTS burst_test_data;
CREATE TABLE burst_test_data (
    userid BIGINT,
    active_time INT
)
    STORED AS ORC;

WITH
    users AS (SELECT posexplode(split(space(4000 - 1), ' ')) AS (seq, val)),
    duplicates AS (SELECT posexplode(split(space(10000 - 1), ' ')) AS (seq, val))
INSERT OVERWRITE TABLE burst_test_data
SELECT
    u.seq AS userid,
    u.seq % 600 AS active_time
FROM
    users u CROSS JOIN duplicates d;

-- TEST --
INSERT OVERWRITE TABLE all_user_online_duration
SELECT userid, get_online_duration(active_time)
FROM burst_test_data
GROUP BY userid;
