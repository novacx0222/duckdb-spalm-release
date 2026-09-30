SET
preserve_insertion_order = false;
SET
disabled_optimizers =
    'compressed_materialization,statistics_propagation';
SET
threads = 1;

CREATE TABLE R
(
    i INTEGER,
    k INTEGER,
    u DOUBLE
);

CREATE TABLE U
(
    k INTEGER,
    j INTEGER,
    v DOUBLE
);

INSERT INTO R
VALUES (100000, 1, 1.0);

INSERT INTO U
VALUES (1, 100000, 1.0);

EXPLAIN
SELECT R.i, U.j, SUM(R.u * U.v) AS total
FROM R
         JOIN U ON R.k = U.k
GROUP BY R.i, U.j;

SELECT R.i, U.j, SUM(R.u * U.v) AS total
FROM R
         JOIN U ON R.k = U.k
GROUP BY R.i, U.j;
