DROP TABLE IF EXISTS concentrated_test_data;
CREATE TABLE concentrated_test_data (
    userid BIGINT,
    active_time INT
)
    STORED AS ORC;

WITH
    users AS (SELECT posexplode(split(space(20000 - 1), ' ')) AS (seq, val)),
    timepoint AS (SELECT posexplode(split(space(1800 - 1), ' ')) AS (seq, val))
INSERT OVERWRITE TABLE concentrated_test_data
SELECT
    u.seq AS userid,
    t.seq AS active_time
FROM
    users u CROSS JOIN timepoint t;

-- TEST --
INSERT OVERWRITE TABLE all_user_online_duration
SELECT userid, get_online_duration(active_time)
FROM concentrated_test_data
GROUP BY userid;
