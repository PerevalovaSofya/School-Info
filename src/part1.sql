DROP TYPE IF EXISTS checkstatus CASCADE;
CREATE TYPE checkstatus AS enum ('start', 'success', 'failure');

DROP TYPE IF EXISTS entrystatus CASCADE;
CREATE TYPE entrystatus AS enum ('1', '2');


DROP TABLE IF EXISTS peers CASCADE;
CREATE TABLE peers
(
    nickname varchar PRIMARY KEY,
    birthday date NOT NULL
);


DROP TABLE IF EXISTS tasks CASCADE;
CREATE TABLE tasks
(
    title      varchar PRIMARY KEY,
    parenttask varchar DEFAULT NULL,
    maxxp      bigint  DEFAULT 0,

    FOREIGN KEY (parenttask) REFERENCES tasks (title)
);


DROP TABLE IF EXISTS checks CASCADE;
CREATE TABLE checks
(
    id     bigserial PRIMARY KEY,
    peer   varchar,
    task   varchar,
    "date" date DEFAULT CURRENT_DATE,

    FOREIGN KEY (peer) REFERENCES peers (nickname),
    FOREIGN KEY (task) REFERENCES tasks (title)
);

DROP TABLE IF EXISTS p2p CASCADE;
CREATE TABLE p2p
(
    id           bigserial PRIMARY KEY,
    checkid      bigint,
    checkingpeer varchar NOT NULL,
    state        checkstatus,
    "time"       time    NOT NULL,

    FOREIGN KEY (checkid) REFERENCES checks (id),
    FOREIGN KEY (checkingpeer) REFERENCES peers (nickname)
);


DROP TABLE IF EXISTS verter CASCADE;
CREATE TABLE verter
(
    id      bigserial PRIMARY KEY,
    checkid bigint,
    state   checkstatus,
    "time"  time NOT NULL,

    FOREIGN KEY (checkid) REFERENCES checks (id)
);


DROP TABLE IF EXISTS transferredpoints CASCADE;
CREATE TABLE transferredpoints
(
    id           bigserial PRIMARY KEY,
    checkingpeer varchar,
    checkedpeer  varchar,
    pointsamount integer,

    FOREIGN KEY (checkingpeer) REFERENCES peers (nickname),
    FOREIGN KEY (checkedpeer) REFERENCES peers (nickname)
);


DROP TABLE IF EXISTS friends CASCADE;
CREATE TABLE friends
(
    id    bigserial PRIMARY KEY,
    peer1 varchar,
    peer2 varchar,

    FOREIGN KEY (peer1) REFERENCES peers (nickname),
    FOREIGN KEY (peer2) REFERENCES peers (nickname)
);


DROP TABLE IF EXISTS recommendations CASCADE;
CREATE TABLE recommendations
(
    id              bigserial PRIMARY KEY,
    peer            varchar,
    recommendedpeer varchar,

    FOREIGN KEY (peer) REFERENCES peers (nickname),
    FOREIGN KEY (recommendedpeer) REFERENCES peers (nickname)

);


DROP TABLE IF EXISTS xp CASCADE;
CREATE TABLE xp
(
    id       bigserial PRIMARY KEY,
    checkid  bigint,
    xpamount integer,

    FOREIGN KEY (checkid) REFERENCES checks (id)
);

DROP TABLE IF EXISTS timetracking CASCADE;
CREATE TABLE timetracking
(
    id     bigserial PRIMARY KEY,
    peer   varchar,
    "date" date,
    "time" time,
    state  entrystatus,

    FOREIGN KEY (peer) REFERENCES peers (nickname)
);

CREATE OR REPLACE FUNCTION fnc_trg_checks_tasks_audit()
    RETURNS trigger AS
$checks_tasks$
BEGIN
    IF (new.date >= (SELECT date
                     FROM checks
                     WHERE checks.peer = new.peer
                       AND checks.task = (SELECT parenttask FROM tasks WHERE tasks.title = new.task))
        OR ((SELECT parenttask FROM tasks WHERE tasks.title = new.task) IS NULL))
    THEN
        RETURN new;
    ELSE
        RAISE EXCEPTION 'The previous project was not completed';
    END IF;
END
$checks_tasks$
    LANGUAGE plpgsql;

CREATE TRIGGER trg_checks_tasks_audit
    BEFORE INSERT
    ON checks
    FOR EACH ROW
EXECUTE FUNCTION fnc_trg_checks_tasks_audit();





CREATE OR REPLACE PROCEDURE import(IN name_table text, IN name_file text, IN delim text)
AS
$$
BEGIN
    EXECUTE FORMAT('COPY %s FROM %L WITH DELIMITER ''%s'' CSV HEADER', $1, $2, $3);
END;
$$ LANGUAGE plpgsql;

SET path.var TO '//Users/ldiomede/Projects/GitHab/SQLinfo/src/data/';

CALL import('peers', CURRENT_SETTING('path.var') || 'peers.csv', ',');
CALL import('transferredpoints', CURRENT_SETTING('path.var') || 'transferred_points.csv', ',');
CALL import('tasks', CURRENT_SETTING('path.var') || 'tasks.csv', ',');
CALL import('checks', CURRENT_SETTING('path.var') || 'checks.csv', ',');
CALL import('p2p', CURRENT_SETTING('path.var') || 'p2p.csv', ',');
CALL import('verter', CURRENT_SETTING('path.var') || 'verter.csv', ',');
CALL import('xp', CURRENT_SETTING('path.var') || 'xp.csv', ',');
CALL import('friends', CURRENT_SETTING('path.var') || 'friends.csv', ',');
CALL import('recommendations', CURRENT_SETTING('path.var') || 'recommendations.csv', ',');
CALL import('timetracking', CURRENT_SETTING('path.var') || 'time_tracking.csv', ',');



CREATE OR REPLACE PROCEDURE export(IN name_table text, IN name_file text, IN delim text)
AS
$$
BEGIN
    EXECUTE FORMAT('COPY %s TO %L WITH DELIMITER ''%s'' CSV HEADER', $1, $2, $3);
END;
$$ LANGUAGE plpgsql;

SET path.var TO '/Users/ldiomede/Projects/SQL/SQLinfo/src/data/';

CALL export('peers', CURRENT_SETTING('path.var') || 'peers.csv', ',');
CALL export('transferredpoints', CURRENT_SETTING('path.var') || 'transferred_points.csv', ',');
CALL export('tasks', CURRENT_SETTING('path.var') || 'tasks.csv', ',');
CALL export('checks', CURRENT_SETTING('path.var') || 'checks.csv', ',');
CALL export('p2p', CURRENT_SETTING('path.var') || 'p2p.csv', ',');
CALL export('verter', CURRENT_SETTING('path.var') || 'verter.csv', ',');
CALL export('xp', CURRENT_SETTING('path.var') || 'xp.csv', ',');
CALL export('friends', CURRENT_SETTING('path.var') || 'friends.csv', ',');
CALL export('recommendations', CURRENT_SETTING('path.var') || 'recommendations.csv', ',');
CALL export('timetracking', CURRENT_SETTING('path.var') || 'time_tracking.csv', ',');






