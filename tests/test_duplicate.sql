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
VALUES (0, 0, 1.0),
       (0, 0, 100.0);

INSERT INTO U
VALUES (0, 0, 2.0),
       (0, 0, 3.0);

SET
preserve_insertion_order = false;
SET
disabled_optimizers =
    'compressed_materialization,statistics_propagation';
SET
threads = 1;

SELECT i, k, COUNT(*) AS multiplicity
FROM R
GROUP BY i, k
HAVING COUNT(*) > 1;

SELECT k, j, COUNT(*) AS multiplicity
FROM U
GROUP BY k, j
HAVING COUNT(*) > 1;

EXPLAIN
SELECT R.i, U.j, SUM(R.u * U.v) AS total
FROM R
         JOIN U ON R.k = U.k
GROUP BY R.i, U.j;

SELECT R.i, U.j, SUM(R.u * U.v) AS total
FROM R
         JOIN U ON R.k = U.k
GROUP BY R.i, U.j;
