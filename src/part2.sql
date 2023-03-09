-- Написать процедуру добавления P2P проверки
CREATE OR REPLACE PROCEDURE p2p_insert(whocome_nick varchar, whocheck_nick varchar, task varchar, status checkstatus,
                                       "time" time)
AS
$$
DECLARE

BEGIN
    IF status = 'start' THEN
        INSERT INTO checks VALUES ((SELECT MAX(id) + 1 FROM checks), whocome_nick, p2p_insert.task, CURRENT_DATE);
        INSERT INTO p2p
        VALUES ((SELECT MAX(id) + 1 FROM p2p), (SELECT MAX(id) FROM checks), whocheck_nick, status, p2p_insert."time");
    ELSE
        INSERT INTO p2p
        VALUES ((SELECT MAX(id) + 1 FROM p2p), (SELECT MAX(checkid)
                                                FROM p2p
                                                WHERE p2p.state = 'start'
                                                  AND p2p.checkingpeer = p2p_insert.whocheck_nick),
                whocheck_nick, status, "time");
    END IF;
END
$$ LANGUAGE plpgsql;


-- Написать процедуру добавления проверки Verter'ом

CREATE OR REPLACE PROCEDURE pr_verter_check(nickname varchar, task_name varchar, verter_state checkstatus,
                                            verter_time time)
AS
$$
DECLARE
    id_check bigint := (SELECT checks.id
                        FROM checks
                                 INNER JOIN p2p
                                            ON checks.id = p2p.checkid
                                                AND p2p.state = 'success'
                                                AND checks.task = task_name
                                                AND checks.peer = nickname
                        ORDER BY p2p."time"
                        LIMIT 1);
BEGIN
    INSERT INTO verter (id, checkid, state, "time")
    VALUES ((SELECT MAX(id) + 1 FROM verter), id_check, verter_state, verter_time);
END
$$ LANGUAGE plpgsql;


-- Написать триггер: после добавления записи со статутом "начало" в таблицу P2P, 
-- изменить соответствующую запись в таблице TransferredPoints



CREATE OR REPLACE FUNCTION fnc_trg_p2p_update_audit()
    RETURNS trigger AS
$$
BEGIN
    UPDATE transferredpoints
    SET pointsamount = pointsamount + 1
    WHERE checkingpeer = new.checkingpeer
      AND new.state = 'start'
      AND checkedpeer = (SELECT peer
                         FROM checks
                                  JOIN p2p ON checks.id = p2p.checkid
                         WHERE checks.id = new.checkid
                           AND state = 'start');
    RETURN new;
END;
$$
    LANGUAGE plpgsql;

CREATE TRIGGER trg_p2p_update_audit
    AFTER INSERT
    ON p2p
    FOR EACH ROW
EXECUTE FUNCTION fnc_trg_p2p_update_audit();

CALL p2p_insert('bperegri', 'rdexter', 'C6_s21_matrix', 'start', '18:31');
CALL p2p_insert('bperegri', 'rdexter', 'C6_s21_matrix', 'success', '18:31');
CALL pr_verter_check('bperegri', 'C6_s21_matrix', 'start', '13:25');
CALL pr_verter_check('bperegri', 'C6_s21_matrix', 'success', '13:26');

SELECT *
FROM transferredpoints
WHERE checkingpeer = 'bperegri'
  AND checkedpeer = 'tcocoa';

-- Написать триггер: перед добавлением записи в таблицу XP, 
-- проверить корректность добавляемой записи
CREATE OR REPLACE FUNCTION fnc_trg_xp_update_audit()
    RETURNS trigger AS
$xp_update$
BEGIN
    IF (new.xpamount > (SELECT maxxp
                        FROM tasks
                                 JOIN checks ON tasks.title = checks.task
                        WHERE checks.id = new.checkid))
        OR NOT EXISTS(SELECT * FROM p2p WHERE state = 'success' AND p2p.checkid = new.checkid)
        OR NOT EXISTS(SELECT * FROM verter WHERE state = 'success' AND verter.checkid = new.checkid)
    THEN
        RAISE EXCEPTION 'Wrong xp amount or check is not success';
    ELSE
        RETURN new;
    END IF;
END
$xp_update$
    LANGUAGE plpgsql;

CREATE TRIGGER trg_xp_update_audit
    BEFORE INSERT
    ON xp
    FOR EACH ROW
EXECUTE FUNCTION fnc_trg_xp_update_audit();

INSERT INTO xp
VALUES ((SELECT MAX(id) + 1 FROM xp), (SELECT MAX(id) FROM checks), '280');

SELECT *
FROM checks
         JOIN xp ON checks.id = checkid
         JOIN p2p ON checks.id = p2p.checkid
         JOIN verter ON checks.id = verter.checkid