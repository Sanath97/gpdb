
-- Tests for queries with order by and limit on B-tree indices.
CREATE TABLE test_index_with_orderby_limit (a int, b int, c float, d int);
CREATE INDEX index_a on test_index_with_orderby_limit using btree(a);
CREATE INDEX index_ab on test_index_with_orderby_limit using btree(a, b);
CREATE INDEX index_bda on test_index_with_orderby_limit using btree(b, d, a);
INSERT INTO test_index_with_orderby_limit select i, i-2, i/3, i+1 from generate_series(1,10000) i;
ANALYZE test_index_with_orderby_limit;
-- should use index scan
explain (costs off) select * from test_index_with_orderby_limit order by a limit 10;
-- should use seq scan
explain (costs off) select * from test_index_with_orderby_limit order by c limit 10;
-- should use index scan
explain (costs off) select * from test_index_with_orderby_limit order by b limit 10;
-- should use index scan
explain (costs off) select * from test_index_with_orderby_limit order by a, b limit 10;
-- should use index scan
explain (costs off) select * from test_index_with_orderby_limit order by b, d limit 10;
-- should use seq scan
explain (costs off) select * from test_index_with_orderby_limit order by d, b limit 10;
-- should use seq scan
explain (costs off) select * from test_index_with_orderby_limit order by d, a limit 10;
-- should use seq scan
explain (costs off) select * from test_index_with_orderby_limit order by a, c limit 10;
-- should use index scan
explain (costs off) select * from test_index_with_orderby_limit order by b, d, a limit 10;
-- should use seq scan
explain (costs off) select * from test_index_with_orderby_limit order by b, d, c limit 10;
-- should use seq scan
explain (costs off) select * from test_index_with_orderby_limit order by c, b, a limit 10;
-- with offset and without limit
explain (costs off) select * from test_index_with_orderby_limit order by a offset 10;
-- limit value in subquery
explain (costs off) select * from test_index_with_orderby_limit order by a limit (select min(a) from test_index_with_orderby_limit);
-- offset value in a subquery
explain (costs off) select * from test_index_with_orderby_limit order by c offset (select min(a) from test_index_with_orderby_limit);
-- order by opposite to index sort direction
explain (costs off) select * from test_index_with_orderby_limit order by a desc limit 10;
-- order by opposite to nulls direction in index
explain (costs off) select * from test_index_with_orderby_limit order by a NULLS FIRST limit 10;
-- check if index-only scan is leveraged when required
-- vacuum table to ensure IndexOnly Scan is picked
VACUUM test_index_with_orderby_limit;
-- project only columns in the Index
explain (costs off) select a from test_index_with_orderby_limit order by a limit 10;

DROP TABLE test_index_with_orderby_limit;

-- Tests for queries with project, predicate and order by on B-tree indices.
CREATE TABLE test_index_with_project_and_predicate (a int, b int, c int, d int);
CREATE INDEX index_a on test_index_with_project_and_predicate using btree(a);
CREATE INDEX index_db on test_index_with_project_and_predicate using btree(d, b);
CREATE INDEX index_bca on test_index_with_project_and_predicate using btree(b, c, a);
INSERT INTO test_index_with_project_and_predicate select i, i-2, i*2, i+1 from generate_series(1,100000) i;
ANALYZE test_index_with_project_and_predicate;
-- All the predicate queries below use IndexScan with IndexCond or Filter
explain (costs off) select * from test_index_with_project_and_predicate where a > 10 order by a limit 4;

explain (costs off) select * from test_index_with_project_and_predicate where c > 40 order by a limit 4;

explain (costs off) select * from test_index_with_project_and_predicate where a < 400 and c > 40 order by a limit 4;

explain (costs off) select * from test_index_with_project_and_predicate where d > 10 order by d limit 4;

explain (costs off) select * from test_index_with_project_and_predicate where c > 10 order by d limit 4;

explain (costs off) select * from test_index_with_project_and_predicate where d > 10 and c > 10 order by d limit 4;

explain (costs off) select * from test_index_with_project_and_predicate where d < 12 order by d, b limit 1;

explain (costs off) select * from test_index_with_project_and_predicate where d > 12  and b > 12 order by d, b limit 4;

explain (costs off) select * from test_index_with_project_and_predicate where a > 12 and c > 100  order by d, b limit 4;

explain (costs off) select * from test_index_with_project_and_predicate where b > 12 order by b, c, a limit 4;

explain (costs off) select * from test_index_with_project_and_predicate where b > 12  and c<100 order by b, c, a limit 4;

explain (costs off) select * from test_index_with_project_and_predicate where b > 12  and c<100  and a>0 order by b, c, a limit 4;

explain (costs off) select * from test_index_with_project_and_predicate where d > 12 order by b, c, a limit 4;

-- query with IndexScan and offset
explain (costs off) select * from test_index_with_project_and_predicate where b > 12  and c<100  and a>0 order by b, c, a offset 4;
-- limit value in subquery
explain (costs off) select * from test_index_with_project_and_predicate where c > 100 order by a limit (select min(a) from test_index_with_project_and_predicate);
-- offset value in a subquery
explain (costs off) select * from test_index_with_project_and_predicate where a > 46 order by d, b  offset (select min(a) from test_index_with_project_and_predicate);
-- order by opposite to index sort direction
explain (costs off) select * from test_index_with_project_and_predicate where a > 68 order by a desc limit 10;
-- order by opposite to nulls direction in index
explain (costs off) select * from test_index_with_project_and_predicate where c > 7 order by d, b NULLS FIRST limit 10;

-- predicate queries with non-index columns in order by
explain (costs off) select * from test_index_with_project_and_predicate where c > 40 order by c limit 4;

explain (costs off) select * from test_index_with_project_and_predicate where a > 12 and c > 100  order by b, d limit 4;

explain (costs off) select * from test_index_with_project_and_predicate where d > 12 order by b, a, c limit 4;

explain (costs off) select * from test_index_with_project_and_predicate where d > 12 order by b, a, d limit 4;

-- Project queries with order by on index columns:
explain (costs off) select a+1 from test_index_with_project_and_predicate order by a limit 3;

explain (costs off) select a, c+3 from test_index_with_project_and_predicate order by a limit 3;

explain (costs off) select a, c+3 from test_index_with_project_and_predicate order by d limit 3;

explain (costs off) select a, c-3 from test_index_with_project_and_predicate order by b limit 3;

explain (costs off) select b, c+3 from test_index_with_project_and_predicate order by d, b limit 3;

explain (costs off) select b+d, c/a from test_index_with_project_and_predicate order by b, c, a limit 3;

-- query with project and offset
explain (costs off) select a, c+3 from test_index_with_project_and_predicate order by b, c, a offset 4;
-- limit value in subquery
explain (costs off) select c*2 from test_index_with_project_and_predicate order by a limit (select min(a) from test_index_with_project_and_predicate);
-- offset value in a subquery
explain (costs off) select a-1 from test_index_with_project_and_predicate order by d, b  offset (select min(a) from test_index_with_project_and_predicate);
-- order by opposite to index sort direction
explain (costs off) select d/7 from test_index_with_project_and_predicate  order by a desc limit 10;
-- order by opposite to nulls direction in index
explain (costs off) select b+1 from test_index_with_project_and_predicate order by d, b NULLS FIRST limit 10;

-- project queries with non-index columns in order by
explain (costs off) select a+c+d from test_index_with_project_and_predicate order by c limit 4;

explain (costs off) select c+1 from test_index_with_project_and_predicate order by b, d limit 4;

explain (costs off) select c+1 from test_index_with_project_and_predicate order by b, a, c limit 4;

DROP TABLE test_index_with_project_and_predicate;