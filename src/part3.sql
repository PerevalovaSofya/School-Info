-- task 3.1
-- Написать функцию, возвращающую таблицу TransferredPoints 
-- в более человекочитаемом виде
CREATE OR REPLACE FUNCTION fnc_transferredpoints()
    RETURNS table
            (
                peer1         varchar,
                peer2         varchar,
                points_amount integer
            )
AS
$$
BEGIN
    RETURN QUERY WITH cte(id1, checking1, checked1, points1) AS (SELECT id, checkingpeer, checkedpeer, pointsamount
                                                                 FROM transferredpoints),
                      cte2(id2, checking2, checked2, points2) AS (SELECT id, checkingpeer, checkedpeer, pointsamount
                                                                  FROM transferredpoints)

                 SELECT checking1           AS peer1,
                        checked1            AS peer2,
                        (points1 - points2) AS pointsamount
                 FROM cte
                          LEFT OUTER JOIN cte2 ON checked1 = checking2 AND checking1 = checked2
                 WHERE (id1 < id2)
                    OR (id2 IS NULL);
END;
$$ LANGUAGE 'plpgsql';

SELECT *
FROM
    fnc_transferredpoints();


-- task 3.2
-- Написать функцию, которая возвращает таблицу вида: ник пользователя, 
-- название проверенного задания, кол-во полученного XP
CREATE OR REPLACE FUNCTION fnc_task_successful()
    RETURNS table
            (
                peer varchar,
                task varchar,
                xp   integer
            )
AS
$$
SELECT checks.peer AS peer,
       checks.task AS task,
       xp.xpamount AS xp
FROM checks
         LEFT JOIN p2p ON checks.id = p2p.checkid
         LEFT JOIN verter ON checks.id = verter.checkid
         JOIN xp ON checks.id = xp.checkid
WHERE p2p.state = 'success'
  AND verter.state = 'success'
ORDER BY peer;
$$ LANGUAGE sql;

SELECT *
FROM
    fnc_task_successful();


-- task 3.3
-- Написать функцию, определяющую пиров, 
-- которые не выходили из кампуса в течение всего дня
CREATE OR REPLACE FUNCTION fnc_full_day(
    date_entry date
)
    RETURNS table
            (
                peer varchar
            )
AS
$$
WITH cte(peer, entry_count) AS (SELECT peer, COUNT(state) FROM timetracking WHERE "date" = date_entry GROUP BY peer)
SELECT peer
FROM cte
WHERE entry_count = 1
$$ LANGUAGE sql;

SELECT *
FROM
    fnc_full_day('2022-09-01');


-- task 3.4
--  Найти процент успешных и неуспешных проверок за всё время
CREATE OR REPLACE PROCEDURE pr_percent_successful(
    ref refcursor
)
    LANGUAGE plpgsql AS
$$
BEGIN
    OPEN ref FOR WITH cte AS (SELECT *
                              FROM checks
                                       JOIN p2p p ON checks.id = p.checkid)
                 SELECT ((SELECT COUNT(state)
                          FROM cte
                          WHERE state = 'success') * 100 / (SELECT COUNT(state)
                                                            FROM cte
                                                            WHERE state = 'start')) AS successfulchecks,
                        ((SELECT COUNT(state)
                          FROM cte
                          WHERE state = 'failure') * 100 / (SELECT COUNT(state)
                                                            FROM cte
                                                            WHERE state = 'start')) AS unsuccessfulchecks;
END;
$$;

BEGIN;
CALL pr_percent_successful('ref');
FETCH ALL IN "ref";
COMMIT;


-- task 3.5
-- Посчитать изменение в количестве пир поинтов каждого пира по таблице TransferredPoints
CREATE PROCEDURE pr_transferred_points(INOUT ref refcursor = 'result')
AS
$$
BEGIN

    OPEN ref FOR SELECT checkingpeer AS peer, (arrival - expense) AS pointschange
                 FROM (SELECT checkingpeer, SUM(pointsamount) AS arrival
                       FROM transferredpoints
                       GROUP BY checkingpeer) AS first
                          JOIN (SELECT checkedpeer, SUM(pointsamount) AS expense
                                FROM transferredpoints
                                GROUP BY checkedpeer) AS second ON first.checkingpeer = second.checkedpeer;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_transferred_points();
FETCH ALL FROM "result";
END;


-- task 3.6
-- Посчитать изменение в количестве пир поинтов каждого пира
CREATE OR REPLACE PROCEDURE pr_count_points(
    ref refcursor
)
    LANGUAGE plpgsql AS
$$
BEGIN
    OPEN ref FOR WITH cte AS (SELECT peer1         AS peer,
                                     points_amount AS points_change
                              FROM
                                  fnc_transferredpoints()
                              UNION ALL
                              SELECT peer2              AS peer,
                                     points_amount * -1 AS points_change
                              FROM
                                  fnc_transferredpoints())

                 SELECT cte.peer,
                        SUM(cte.points_change) AS points_change
                 FROM cte
                 GROUP BY cte.peer
                 ORDER BY points_change DESC, cte.peer;
END;
$$;

BEGIN;
CALL pr_count_points('ref');
FETCH ALL IN "ref";
COMMIT;


-- task 3.7
-- Определить самое часто проверяемое задание за каждый день
CREATE OR REPLACE PROCEDURE pr_the_most_checked_task(INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR SELECT first."date" AS day, task
                 FROM (SELECT COUNT(*) AS count, task, "date" FROM checks AS first GROUP BY "date", task) AS first
                          JOIN (SELECT mid."date", MAX(count) AS max
                                FROM (SELECT COUNT(*) AS count, task, "date" FROM checks GROUP BY "date", task) AS mid
                                GROUP BY "date") AS second ON first."date" = second."date" AND first.count = second.max;
END
$$ LANGUAGE plpgsql;


BEGIN;
CALL pr_the_most_checked_task();
FETCH ALL FROM "result";
END;


-- task 3.8
-- Определить длительность последней P2P проверки
CREATE PROCEDURE pr_duration_last_p2p(INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR SELECT (first.time - second.time)::time AS duration_p2p
                 FROM p2p first
                          JOIN p2p second
                               ON first.checkid = second.checkid
                 WHERE first.state <> 'start'
                   AND second.state = 'start'
                 ORDER BY first.id DESC
                 LIMIT 1;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_duration_last_p2p();
FETCH ALL FROM "result";
END;



-- task 3.9
-- Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания
CREATE OR REPLACE PROCEDURE pr_peers_ending_block(taskname varchar, INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR SELECT peer AS peer, date AS day
                 FROM checks
                          JOIN verter v ON checks.id = v.checkid
                 WHERE state = 'success'
                   AND checks.task = (SELECT MAX(title)
                                      FROM (SELECT UNNEST(REGEXP_MATCHES(checks.task, CONCAT('(', taskname, '\d.*)'))) AS title
                                            FROM checks) AS zhopa)
                 ORDER BY date;
END
$$ LANGUAGE plpgsql;


BEGIN;
CALL pr_peers_ending_block('CPP');
FETCH ALL FROM "result";
END;


-- task 3.10
-- Определить, к какому пиру стоит идти на проверку каждому обучающемуся
CREATE OR REPLACE PROCEDURE pr_peers_recomendations(INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR SELECT DISTINCT ON (first.nickname) first.nickname AS peer, first.recommendedpeer AS recommendedpeer
                 FROM (SELECT peers.nickname, rec.recommendedpeer, COUNT(rec.recommendedpeer) AS num
                       FROM peers
                                JOIN friends ON peers.nickname = friends.peer1 OR
                                                peers.nickname = friends.peer2
                                JOIN recommendations AS rec
                                     ON (friends.peer1 = rec.peer AND friends.peer1 != peers.nickname) OR
                                        (friends.peer2 = rec.peer AND friends.peer2 != peers.nickname)
                       WHERE peers.nickname != rec.recommendedpeer
                       GROUP BY peers.nickname, rec.recommendedpeer) AS first
                 ORDER BY first.nickname, first.num DESC;


END
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_peers_recomendations();
FETCH ALL IN "result";
END;


-- task 3.11
-- Определить процент пиров, которые приступили только к определенным блокам заданий
CREATE OR REPLACE PROCEDURE pr_peers_who_start_block(taskname varchar, taskname_2 varchar, INOUT ref refcursor = 'result')
AS
$$
DECLARE
    all_peers         bigint := (SELECT COUNT(*)
                                 FROM peers);
    block_1_peers     bigint := (SELECT COUNT(peer)
                                 FROM ((SELECT DISTINCT peer
                                        FROM peers
                                                 JOIN checks ON peers.nickname = checks.peer
                                        WHERE checks.task =
                                              (SELECT UNNEST(REGEXP_MATCHES(checks.task, CONCAT('(', taskname, '\d.*)'))) AS title))
                                       EXCEPT
                                       (SELECT DISTINCT peer
                                        FROM peers
                                                 JOIN checks ON peers.nickname = checks.peer
                                        WHERE checks.task =
                                              (SELECT UNNEST(REGEXP_MATCHES(checks.task, CONCAT('(', taskname_2, '\d.*)'))) AS title))) AS f);
    block_2_peers     bigint := (SELECT COUNT(peer)
                                 FROM ((SELECT DISTINCT peer
                                        FROM peers
                                                 JOIN checks ON peers.nickname = checks.peer
                                        WHERE checks.task =
                                              (SELECT UNNEST(REGEXP_MATCHES(checks.task, CONCAT('(', taskname_2, '\d.*)'))) AS title))
                                       EXCEPT
                                       (SELECT DISTINCT peer
                                        FROM peers
                                                 JOIN checks ON peers.nickname = checks.peer
                                        WHERE checks.task =
                                              (SELECT UNNEST(REGEXP_MATCHES(checks.task, CONCAT('(', taskname, '\d.*)'))) AS title))) AS f);
    both_blocks_peers bigint := COUNT((SELECT DISTINCT peer
                                       FROM peers
                                                JOIN checks ON peers.nickname = checks.peer
                                       WHERE checks.task =
                                             (SELECT UNNEST(REGEXP_MATCHES(checks.task, CONCAT('(', taskname, '\d.*)'))) AS title)
                                       INTERSECT
                                       (SELECT DISTINCT peer
                                        FROM peers
                                                 JOIN checks ON peers.nickname = checks.peer
                                        WHERE checks.task =
                                              (SELECT UNNEST(REGEXP_MATCHES(checks.task, CONCAT('(', taskname_2, '\d.*)'))) AS title))));

BEGIN
    OPEN ref FOR SELECT (block_1_peers / all_peers::float) * 100     AS startedblock1,
                        (block_2_peers / all_peers::float) * 100     AS startedblock2,
                        (both_blocks_peers / all_peers::float) * 100 AS startedbothblocks,
                        ((all_peers - (block_1_peers + block_2_peers + both_blocks_peers)) / all_peers::float) *
                        100                                          AS didntstartanyblock;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_peers_who_start_block('CPP', 'C');
FETCH ALL IN "result";
END;


-- task 3.12
-- Определить *N* пиров с наибольшим числом друзей
CREATE PROCEDURE pr_count_friend(INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR SELECT nickname AS peer, COUNT(first.peer1) + COUNT(second.peer2) AS friendscount
                 FROM peers
                          LEFT JOIN friends first ON peers.nickname = first.peer1
                          LEFT JOIN friends second ON peers.nickname = second.peer2
                 GROUP BY peers.nickname
                 ORDER BY friendscount DESC;

END
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_count_friend();
FETCH ALL FROM "result";
END;



-- task 3.13
-- Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения
CREATE OR REPLACE PROCEDURE pr_percent_successful_birthday(
    ref refcursor
)
    LANGUAGE plpgsql AS
$$
BEGIN
    OPEN ref FOR WITH cte AS (SELECT *
                              FROM checks
                                       JOIN p2p ON checks.id = p2p.checkid
                                       JOIN peers ON checks.peer = peers.nickname

                              WHERE DATE_PART('day', birthday) = DATE_PART('day', "date")
                                AND DATE_PART('month', birthday) = DATE_PART('month', "date"))

                 SELECT COALESCE(((SELECT COUNT(state)
                          FROM cte
                          WHERE state = 'success') * 100 / NULLIF((SELECT COUNT(state)
                                                            FROM cte
                                                            WHERE state = 'start'), 0)), 0) AS successfulchecks,
                        COALESCE(((SELECT COUNT(state)
                          FROM cte
                          WHERE state = 'failure') * 100 / NULLIF((SELECT COUNT(state)
                                                            FROM cte
                                                            WHERE state = 'start'), 0)), 0) AS unsuccessfulchecks;

END;
$$;

BEGIN;
CALL pr_percent_successful_birthday('ref');
FETCH ALL IN "ref";
COMMIT;


-- task 3.14
-- Определить кол-во XP, полученное в сумме каждым пиром
CREATE OR REPLACE PROCEDURE pr_max_xp(INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR SELECT peers.nickname AS peer, SUM(tasks.maxxp) AS xp
                 FROM peers
                          JOIN checks ON peers.nickname = checks.peer
                          JOIN verter v ON checks.id = v.checkid
                          JOIN tasks ON checks.task = tasks.title
                 GROUP BY peers.nickname
                 ORDER BY xp;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_max_xp();
FETCH ALL IN "result";
END;


-- task 3.15
-- Определить всех пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3
CREATE OR REPLACE PROCEDURE pr_find_completed_tasks(first_task varchar, second_task varchar, third_task varchar,
                                                    INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR (SELECT peer
                  FROM checks
                           JOIN p2p p ON checks.id = p.checkid
                           JOIN verter v ON checks.id = v.checkid
                  WHERE checks.task = first_task
                    AND v.state = 'success'
                    AND p.state = 'success')
                 INTERSECT
                 (SELECT peer
                  FROM checks
                           JOIN p2p p ON checks.id = p.checkid
                           JOIN verter v ON checks.id = v.checkid
                  WHERE checks.task = second_task
                    AND v.state = 'success'
                    AND p.state = 'success')
                 EXCEPT
                 (SELECT peer
                  FROM checks
                           JOIN p2p p ON checks.id = p.checkid
                           JOIN verter v ON checks.id = v.checkid
                  WHERE checks.task = third_task
                    AND v.state = 'success'
                    AND p.state = 'success');
END
$$ LANGUAGE plpgsql;


BEGIN;
CALL pr_find_completed_tasks('C5_s21_decimal', 'C3_s21_string+', 'C6_s21_matrix');
FETCH ALL FROM "result";
END;


-- task 3.16
-- Используя рекурсивное обобщенное табличное выражение, 
-- для каждой задачи вывести кол-во предшествующих ей задач
CREATE OR REPLACE PROCEDURE prev_tasks(INOUT curs refcursor = 'result') AS
$$
BEGIN
    OPEN curs FOR WITH RECURSIVE previous_task(title, parenttask, count) AS
                                     (SELECT title, parenttask, 0
                                      FROM tasks
                                      WHERE parenttask IS NULL
                                      UNION ALL
                                      SELECT tasks.title, tasks.parenttask, count + 1
                                      FROM previous_task pt,
                                           tasks
                                      WHERE pt.title = tasks.parenttask)
                  SELECT title AS task, count AS prevcount
                  FROM previous_task;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prev_tasks();
FETCH ALL IN "result";
END;


-- task 3.17
-- Найти "удачные" для проверок дни. День считается "удачным", если в нем есть хотя бы *N* идущих подряд успешных проверки
CREATE PROCEDURE pr_success_checks(count_checks bigint, INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR SELECT DISTINCT date
                 FROM (SELECT date, ROW_NUMBER() OVER (PARTITION BY date, status, count ORDER BY date, count) AS count
                       FROM (SELECT date,
                                    v.state                                     AS status,
                                    xpamount,
                                    maxxp,
                                    COUNT(
                                    CASE
                                        WHEN ((xpamount::real / maxxp::real) * 100) < 80 THEN 1
                                        WHEN v.state <> 'success' THEN 1
                                        END)
                                    OVER (PARTITION BY date ORDER BY v.checkid) AS count
                             FROM checks
                                      JOIN verter v ON checks.id = v.checkid
                                      JOIN tasks t ON checks.task = t.title
                                      LEFT JOIN xp x ON checks.id = x.checkid
                             WHERE v.state <> 'start'
                             ORDER BY date) second
                       WHERE status <> 'failure'
                         AND ((xpamount::real / maxxp::real) * 100) >= 80) third
                 WHERE count >= count_checks;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_success_checks(1);
FETCH ALL FROM "result";
END;


-- task 3.18
-- Определить пира с наибольшим числом выполненных заданий
CREATE PROCEDURE pr_count_project(INOUT ref refcursor = 'result')
AS
$$
BEGIN

    OPEN ref FOR SELECT peer, COUNT(task) AS count_project
                 FROM checks
                          JOIN p2p ON checks.id = p2p.checkid
                          JOIN verter ON checks.id = verter.checkid
                 WHERE verter.state = 'success'
                   AND p2p.state = 'success'
                 GROUP BY peer
                 ORDER BY count_project DESC
                 LIMIT 1;
END
$$ LANGUAGE plpgsql;


BEGIN;
CALL pr_count_project();
FETCH ALL FROM "result";
END;


-- task 3.19
-- Определить пира с наибольшим количеством XP
CREATE PROCEDURE pr_count_xp(INOUT ref refcursor = 'result')
AS
$$
BEGIN

    OPEN ref FOR SELECT peer, SUM(xpamount) AS count_project
                 FROM checks
                          JOIN p2p ON checks.id = p2p.checkid
                          JOIN verter ON checks.id = verter.checkid
                          JOIN xp ON checks.id = xp.checkid
                 WHERE verter.state = 'success'
                   AND p2p.state = 'success'
                 GROUP BY peer
                 ORDER BY count_project DESC
                 LIMIT 1;
END
$$ LANGUAGE plpgsql;


BEGIN;
CALL pr_count_xp();
FETCH ALL FROM "result";
END;


-- task 3.20
-- Определить пира, который провел сегодня в кампусе больше всего времени
CREATE OR REPLACE PROCEDURE pr_max_by_time(entry_date date, INOUT curs refcursor = 'result') AS
$$
BEGIN
    OPEN curs FOR SELECT peer
                  FROM (WITH all_peers AS (SELECT * FROM timetracking WHERE date = entry_date),
                             enter AS (SELECT peer, SUM(time) AS entry_time
                                       FROM all_peers
                                       WHERE state = '1'
                                       GROUP BY peer),
                             exit AS (SELECT peer, SUM(time) AS exit_time
                                      FROM all_peers
                                      WHERE state = '2'
                                      GROUP BY peer)
                        SELECT enter.peer, (exit.exit_time - enter.entry_time) AS time_in_campus
                        FROM enter
                                 JOIN exit ON enter.peer = exit.peer
                        ORDER BY time_in_campus DESC
                        LIMIT 1) AS peer_time;
END;
$$ LANGUAGE plpgsql;


BEGIN;
CALL pr_max_by_time('2022-09-01');
FETCH ALL IN "result";
END;


-- task 3.21
-- Определить пиров, приходивших раньше заданного времени не менее *N* раз за всё время
CREATE OR REPLACE PROCEDURE pr_entry_time(
    check_time time, n bigint, ref refcursor
) AS
$$
BEGIN
    OPEN ref FOR SELECT peer
                 FROM timetracking
                 WHERE "time" < check_time
                   AND state = '1'
                 GROUP BY peer
                 HAVING COUNT(state) >= n;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_entry_time('20:00:00', '2', 'ref');
FETCH ALL IN "ref";
COMMIT;
END;


-- task 3.22
-- Определить пиров, выходивших за последние *N* дней из кампуса больше *M* раз
CREATE OR REPLACE PROCEDURE pr_peer_who_exit(entry_date date, n integer, m integer, INOUT curs refcursor = 'result') AS
$$
BEGIN
    OPEN curs FOR SELECT peer
                  FROM (SELECT peer, date, COUNT(*) AS count_
                        FROM timetracking
                        WHERE state = '2'
                          AND date >= (entry_date::date - n)
                        GROUP BY peer, date) AS first
                  GROUP BY peer
                  HAVING SUM(count_) > m
                  ORDER BY peer;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_peer_who_exit('2022-09-01', 5, 0);
FETCH ALL IN "result";
END;


-- task 3.23
-- Определить пира, который пришел сегодня последним
CREATE PROCEDURE pr_last_peer(INOUT ref refcursor = 'result')
AS
$$
BEGIN

    OPEN ref FOR SELECT peer
                 FROM (SELECT DISTINCT peer, time
                       FROM timetracking
                       WHERE date = CURRENT_DATE
                         AND state = '1'
                       ORDER BY peer, time) AS peer_time
                 ORDER BY time DESC
                 LIMIT 1;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_last_peer();
FETCH ALL FROM "result";
END;


-- task 3.24
-- Определить пиров, которые выходили вчера из кампуса больше чем на *N* минут
CREATE OR REPLACE PROCEDURE pr_exit_time(
    minute bigint, ref refcursor
) AS
$$
BEGIN
    OPEN ref FOR WITH entry AS (SELECT *
                                FROM timetracking
                                WHERE timetracking.state = '1'
                                  AND "date" = CURRENT_DATE - '1 day'::interval),
                      exit AS (SELECT *
                               FROM timetracking
                               WHERE timetracking.state = '2'

                                 AND "date" = CURRENT_DATE - '1 day'::interval)

                 SELECT entry.peer AS peer
                 FROM entry
                          INNER JOIN exit ON entry.peer = exit.peer
                 WHERE exit.id < entry.id
                   AND (entry.time - exit.time) < minute * '1 minute'::interval;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_exit_time(20, 'ref');
FETCH ALL IN "ref";
COMMIT;
END;


-- task 3.25
-- Определить для каждого месяца процент ранних входов
CREATE OR REPLACE PROCEDURE pr_early_came_percent(INOUT curs refcursor = 'result') AS
$$
BEGIN
    OPEN curs FOR WITH months AS (SELECT ROW_NUMBER() OVER () AS number, TO_CHAR(gs, 'Month') AS month
                                  FROM (SELECT generate_series AS gs
                                        FROM GENERATE_SERIES('2023-01-01', '2023-12-31', INTERVAL '1 month')) AS series)

                  SELECT month,
                         COALESCE((SELECT COUNT(*) * 100 / NULLIF((SELECT COUNT(*)
                                                                   FROM timetracking
                                                                            JOIN peers ON peers.nickname = timetracking.peer
                                                                   WHERE state = '1'
                                                                     AND (SELECT SUBSTRING(TO_CHAR(timetracking.date, 'yyyy-mm-dd') FROM 6 FOR 2)) =
                                                                         (SELECT SUBSTRING(TO_CHAR(peers.birthday, 'yyyy-mm-dd') FROM 6 FOR 2))
                                                                     AND number = EXTRACT(MONTH FROM timetracking.date)),
                                                                  0)

                                   FROM peers
                                            JOIN timetracking ON peers.nickname = timetracking.peer
                                   WHERE (SELECT SUBSTRING(TO_CHAR(timetracking.date, 'yyyy-mm-dd') FROM 6 FOR 2)) =
                                         (SELECT SUBSTRING(TO_CHAR(peers.birthday, 'yyyy-mm-dd') FROM 6 FOR 2))
                                     AND timetracking.state = '1'
                                     AND EXTRACT(HOURS FROM timetracking.time) < 12
                                     AND number = EXTRACT(MONTH FROM timetracking.date)), 0) AS earlyentries
                  FROM months;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_early_came_percent();
FETCH ALL IN "result";
END;

