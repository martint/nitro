/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.weakref.nitro.tpch;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * The 22 TPC-H queries with the spec's validation substitution parameters, parameterized by catalog and
 * schema. Embedded (rather than read from a Trino checkout like the TPC-DS catalog) so the suite is
 * self-contained; the texts are the standard qgen output with Trino syntax (explicit interval arithmetic,
 * {@code LIMIT} in place of {@code FIRST -1}, a CTE for Q15's view). Q11's fraction follows the spec's
 * 0.0001 / SF scaling for the sf10 data (the unscaled validation value returns zero rows at this scale).
 */
public final class TpchQueryCatalog
{
    private TpchQueryCatalog() {}

    public static List<String> queryIds()
    {
        return IntStream.rangeClosed(1, 22).mapToObj(value -> String.format("%02d", value)).toList();
    }

    public static String sql(String queryId, String catalog, String schema)
    {
        String text = QUERIES.get(normalize(queryId));
        if (text == null) {
            throw new IllegalArgumentException("Unknown TPC-H query: " + queryId);
        }
        return text.replace("${prefix}", catalog + "." + schema + ".");
    }

    private static String normalize(String queryId)
    {
        return String.format("%02d", Integer.parseInt(queryId));
    }

    private static final Map<String, String> QUERIES = Map.ofEntries(
            Map.entry("01", """
                    SELECT
                      l_returnflag,
                      l_linestatus,
                      sum(l_quantity) AS sum_qty,
                      sum(l_extendedprice) AS sum_base_price,
                      sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                      sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                      avg(l_quantity) AS avg_qty,
                      avg(l_extendedprice) AS avg_price,
                      avg(l_discount) AS avg_disc,
                      count(*) AS count_order
                    FROM ${prefix}lineitem
                    WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
                    GROUP BY l_returnflag, l_linestatus
                    ORDER BY l_returnflag, l_linestatus
                    """),
            Map.entry("02", """
                    SELECT
                      s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
                    FROM ${prefix}part, ${prefix}supplier, ${prefix}partsupp, ${prefix}nation, ${prefix}region
                    WHERE p_partkey = ps_partkey
                      AND s_suppkey = ps_suppkey
                      AND p_size = 15
                      AND p_type LIKE '%BRASS'
                      AND s_nationkey = n_nationkey
                      AND n_regionkey = r_regionkey
                      AND r_name = 'EUROPE'
                      AND ps_supplycost = (
                        SELECT min(ps_supplycost)
                        FROM ${prefix}partsupp, ${prefix}supplier, ${prefix}nation, ${prefix}region
                        WHERE p_partkey = ps_partkey
                          AND s_suppkey = ps_suppkey
                          AND s_nationkey = n_nationkey
                          AND n_regionkey = r_regionkey
                          AND r_name = 'EUROPE')
                    ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
                    LIMIT 100
                    """),
            Map.entry("03", """
                    SELECT
                      l_orderkey,
                      sum(l_extendedprice * (1 - l_discount)) AS revenue,
                      o_orderdate,
                      o_shippriority
                    FROM ${prefix}customer, ${prefix}orders, ${prefix}lineitem
                    WHERE c_mktsegment = 'BUILDING'
                      AND c_custkey = o_custkey
                      AND l_orderkey = o_orderkey
                      AND o_orderdate < DATE '1995-03-15'
                      AND l_shipdate > DATE '1995-03-15'
                    GROUP BY l_orderkey, o_orderdate, o_shippriority
                    ORDER BY revenue DESC, o_orderdate
                    LIMIT 10
                    """),
            Map.entry("04", """
                    SELECT o_orderpriority, count(*) AS order_count
                    FROM ${prefix}orders
                    WHERE o_orderdate >= DATE '1993-07-01'
                      AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
                      AND EXISTS (
                        SELECT *
                        FROM ${prefix}lineitem
                        WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
                    GROUP BY o_orderpriority
                    ORDER BY o_orderpriority
                    """),
            Map.entry("05", """
                    SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue
                    FROM ${prefix}customer, ${prefix}orders, ${prefix}lineitem, ${prefix}supplier, ${prefix}nation, ${prefix}region
                    WHERE c_custkey = o_custkey
                      AND l_orderkey = o_orderkey
                      AND l_suppkey = s_suppkey
                      AND c_nationkey = s_nationkey
                      AND s_nationkey = n_nationkey
                      AND n_regionkey = r_regionkey
                      AND r_name = 'ASIA'
                      AND o_orderdate >= DATE '1994-01-01'
                      AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
                    GROUP BY n_name
                    ORDER BY revenue DESC
                    """),
            Map.entry("06", """
                    SELECT sum(l_extendedprice * l_discount) AS revenue
                    FROM ${prefix}lineitem
                    WHERE l_shipdate >= DATE '1994-01-01'
                      AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
                      AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
                      AND l_quantity < 24
                    """),
            Map.entry("07", """
                    SELECT supp_nation, cust_nation, l_year, sum(volume) AS revenue
                    FROM (
                      SELECT
                        n1.n_name AS supp_nation,
                        n2.n_name AS cust_nation,
                        EXTRACT(YEAR FROM l_shipdate) AS l_year,
                        l_extendedprice * (1 - l_discount) AS volume
                      FROM ${prefix}supplier, ${prefix}lineitem, ${prefix}orders, ${prefix}customer, ${prefix}nation n1, ${prefix}nation n2
                      WHERE s_suppkey = l_suppkey
                        AND o_orderkey = l_orderkey
                        AND c_custkey = o_custkey
                        AND s_nationkey = n1.n_nationkey
                        AND c_nationkey = n2.n_nationkey
                        AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                          OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
                        AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31') AS shipping
                    GROUP BY supp_nation, cust_nation, l_year
                    ORDER BY supp_nation, cust_nation, l_year
                    """),
            Map.entry("08", """
                    SELECT
                      o_year,
                      sum(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / sum(volume) AS mkt_share
                    FROM (
                      SELECT
                        EXTRACT(YEAR FROM o_orderdate) AS o_year,
                        l_extendedprice * (1 - l_discount) AS volume,
                        n2.n_name AS nation
                      FROM ${prefix}part, ${prefix}supplier, ${prefix}lineitem, ${prefix}orders, ${prefix}customer, ${prefix}nation n1, ${prefix}nation n2, ${prefix}region
                      WHERE p_partkey = l_partkey
                        AND s_suppkey = l_suppkey
                        AND l_orderkey = o_orderkey
                        AND o_custkey = c_custkey
                        AND c_nationkey = n1.n_nationkey
                        AND n1.n_regionkey = r_regionkey
                        AND r_name = 'AMERICA'
                        AND s_nationkey = n2.n_nationkey
                        AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                        AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations
                    GROUP BY o_year
                    ORDER BY o_year
                    """),
            Map.entry("09", """
                    SELECT nation, o_year, sum(amount) AS sum_profit
                    FROM (
                      SELECT
                        n_name AS nation,
                        EXTRACT(YEAR FROM o_orderdate) AS o_year,
                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
                      FROM ${prefix}part, ${prefix}supplier, ${prefix}lineitem, ${prefix}partsupp, ${prefix}orders, ${prefix}nation
                      WHERE s_suppkey = l_suppkey
                        AND ps_suppkey = l_suppkey
                        AND ps_partkey = l_partkey
                        AND p_partkey = l_partkey
                        AND o_orderkey = l_orderkey
                        AND s_nationkey = n_nationkey
                        AND p_name LIKE '%green%') AS profit
                    GROUP BY nation, o_year
                    ORDER BY nation, o_year DESC
                    """),
            Map.entry("10", """
                    SELECT
                      c_custkey, c_name,
                      sum(l_extendedprice * (1 - l_discount)) AS revenue,
                      c_acctbal, n_name, c_address, c_phone, c_comment
                    FROM ${prefix}customer, ${prefix}orders, ${prefix}lineitem, ${prefix}nation
                    WHERE c_custkey = o_custkey
                      AND l_orderkey = o_orderkey
                      AND o_orderdate >= DATE '1993-10-01'
                      AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH
                      AND l_returnflag = 'R'
                      AND c_nationkey = n_nationkey
                    GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
                    ORDER BY revenue DESC
                    LIMIT 20
                    """),
            Map.entry("11", """
                    SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS value
                    FROM ${prefix}partsupp, ${prefix}supplier, ${prefix}nation
                    WHERE ps_suppkey = s_suppkey
                      AND s_nationkey = n_nationkey
                      AND n_name = 'GERMANY'
                    GROUP BY ps_partkey
                    HAVING sum(ps_supplycost * ps_availqty) > (
                      SELECT sum(ps_supplycost * ps_availqty) * 0.00001
                      FROM ${prefix}partsupp, ${prefix}supplier, ${prefix}nation
                      WHERE ps_suppkey = s_suppkey
                        AND s_nationkey = n_nationkey
                        AND n_name = 'GERMANY')
                    ORDER BY value DESC
                    """),
            Map.entry("12", """
                    SELECT
                      l_shipmode,
                      sum(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count,
                      sum(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count
                    FROM ${prefix}orders, ${prefix}lineitem
                    WHERE o_orderkey = l_orderkey
                      AND l_shipmode IN ('MAIL', 'SHIP')
                      AND l_commitdate < l_receiptdate
                      AND l_shipdate < l_commitdate
                      AND l_receiptdate >= DATE '1994-01-01'
                      AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1' YEAR
                    GROUP BY l_shipmode
                    ORDER BY l_shipmode
                    """),
            Map.entry("13", """
                    SELECT c_count, count(*) AS custdist
                    FROM (
                      SELECT c_custkey, count(o_orderkey) AS c_count
                      FROM ${prefix}customer
                      LEFT OUTER JOIN ${prefix}orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
                      GROUP BY c_custkey) AS c_orders
                    GROUP BY c_count
                    ORDER BY custdist DESC, c_count DESC
                    """),
            Map.entry("14", """
                    SELECT 100.00 * sum(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
                        / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
                    FROM ${prefix}lineitem, ${prefix}part
                    WHERE l_partkey = p_partkey
                      AND l_shipdate >= DATE '1995-09-01'
                      AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH
                    """),
            Map.entry("15", """
                    WITH revenue0 AS (
                      SELECT l_suppkey AS supplier_no, sum(l_extendedprice * (1 - l_discount)) AS total_revenue
                      FROM ${prefix}lineitem
                      WHERE l_shipdate >= DATE '1996-01-01'
                        AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
                      GROUP BY l_suppkey)
                    SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
                    FROM ${prefix}supplier, revenue0
                    WHERE s_suppkey = supplier_no
                      AND total_revenue = (SELECT max(total_revenue) FROM revenue0)
                    ORDER BY s_suppkey
                    """),
            Map.entry("16", """
                    SELECT p_brand, p_type, p_size, count(DISTINCT ps_suppkey) AS supplier_cnt
                    FROM ${prefix}partsupp, ${prefix}part
                    WHERE p_partkey = ps_partkey
                      AND p_brand <> 'Brand#45'
                      AND p_type NOT LIKE 'MEDIUM POLISHED%'
                      AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
                      AND ps_suppkey NOT IN (
                        SELECT s_suppkey
                        FROM ${prefix}supplier
                        WHERE s_comment LIKE '%Customer%Complaints%')
                    GROUP BY p_brand, p_type, p_size
                    ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
                    """),
            Map.entry("17", """
                    SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
                    FROM ${prefix}lineitem, ${prefix}part
                    WHERE p_partkey = l_partkey
                      AND p_brand = 'Brand#23'
                      AND p_container = 'MED BOX'
                      AND l_quantity < (
                        SELECT 0.2 * avg(l_quantity)
                        FROM ${prefix}lineitem
                        WHERE l_partkey = p_partkey)
                    """),
            Map.entry("18", """
                    SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
                    FROM ${prefix}customer, ${prefix}orders, ${prefix}lineitem
                    WHERE o_orderkey IN (
                        SELECT l_orderkey
                        FROM ${prefix}lineitem
                        GROUP BY l_orderkey
                        HAVING sum(l_quantity) > 300)
                      AND c_custkey = o_custkey
                      AND o_orderkey = l_orderkey
                    GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
                    ORDER BY o_totalprice DESC, o_orderdate
                    LIMIT 100
                    """),
            Map.entry("19", """
                    SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue
                    FROM ${prefix}lineitem, ${prefix}part
                    WHERE (p_partkey = l_partkey
                        AND p_brand = 'Brand#12'
                        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                        AND l_quantity >= 1 AND l_quantity <= 1 + 10
                        AND p_size BETWEEN 1 AND 5
                        AND l_shipmode IN ('AIR', 'AIR REG')
                        AND l_shipinstruct = 'DELIVER IN PERSON')
                      OR (p_partkey = l_partkey
                        AND p_brand = 'Brand#23'
                        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                        AND l_quantity >= 10 AND l_quantity <= 10 + 10
                        AND p_size BETWEEN 1 AND 10
                        AND l_shipmode IN ('AIR', 'AIR REG')
                        AND l_shipinstruct = 'DELIVER IN PERSON')
                      OR (p_partkey = l_partkey
                        AND p_brand = 'Brand#34'
                        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                        AND l_quantity >= 20 AND l_quantity <= 20 + 10
                        AND p_size BETWEEN 1 AND 15
                        AND l_shipmode IN ('AIR', 'AIR REG')
                        AND l_shipinstruct = 'DELIVER IN PERSON')
                    """),
            Map.entry("20", """
                    SELECT s_name, s_address
                    FROM ${prefix}supplier, ${prefix}nation
                    WHERE s_suppkey IN (
                        SELECT ps_suppkey
                        FROM ${prefix}partsupp
                        WHERE ps_partkey IN (
                            SELECT p_partkey
                            FROM ${prefix}part
                            WHERE p_name LIKE 'forest%')
                          AND ps_availqty > (
                            SELECT 0.5 * sum(l_quantity)
                            FROM ${prefix}lineitem
                            WHERE l_partkey = ps_partkey
                              AND l_suppkey = ps_suppkey
                              AND l_shipdate >= DATE '1994-01-01'
                              AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR))
                      AND s_nationkey = n_nationkey
                      AND n_name = 'CANADA'
                    ORDER BY s_name
                    """),
            Map.entry("21", """
                    SELECT s_name, count(*) AS numwait
                    FROM ${prefix}supplier, ${prefix}lineitem l1, ${prefix}orders, ${prefix}nation
                    WHERE s_suppkey = l1.l_suppkey
                      AND o_orderkey = l1.l_orderkey
                      AND o_orderstatus = 'F'
                      AND l1.l_receiptdate > l1.l_commitdate
                      AND EXISTS (
                        SELECT *
                        FROM ${prefix}lineitem l2
                        WHERE l2.l_orderkey = l1.l_orderkey
                          AND l2.l_suppkey <> l1.l_suppkey)
                      AND NOT EXISTS (
                        SELECT *
                        FROM ${prefix}lineitem l3
                        WHERE l3.l_orderkey = l1.l_orderkey
                          AND l3.l_suppkey <> l1.l_suppkey
                          AND l3.l_receiptdate > l3.l_commitdate)
                      AND s_nationkey = n_nationkey
                      AND n_name = 'SAUDI ARABIA'
                    GROUP BY s_name
                    ORDER BY numwait DESC, s_name
                    LIMIT 100
                    """),
            Map.entry("22", """
                    SELECT cntrycode, count(*) AS numcust, sum(c_acctbal) AS totacctbal
                    FROM (
                      SELECT substring(c_phone, 1, 2) AS cntrycode, c_acctbal
                      FROM ${prefix}customer
                      WHERE substring(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
                        AND c_acctbal > (
                          SELECT avg(c_acctbal)
                          FROM ${prefix}customer
                          WHERE c_acctbal > 0.00
                            AND substring(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17'))
                        AND NOT EXISTS (
                          SELECT *
                          FROM ${prefix}orders
                          WHERE o_custkey = c_custkey)) AS custsale
                    GROUP BY cntrycode
                    ORDER BY cntrycode
                    """));
}
