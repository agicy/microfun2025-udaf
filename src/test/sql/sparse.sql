DROP TABLE IF EXISTS sparse_test_data;
CREATE TABLE sparse_test_data (
    userid BIGINT,
    active_time INT
)
    STORED AS ORC;

WITH
    users AS (SELECT posexplode(split(space(100000 - 1), ' ')) AS (seq, val)),
    block AS (SELECT posexplode(split(space(144 - 1), ' ')) AS (seq, val))
INSERT OVERWRITE TABLE sparse_test_data
SELECT
    u.seq AS userid,
    (b.seq * 600) - (b.seq % 2) AS active_time
FROM
    users u CROSS JOIN block b;

-- TEST --
INSERT OVERWRITE TABLE all_user_online_duration
SELECT userid, get_online_duration(active_time)
FROM sparse_test_data
GROUP BY userid;
