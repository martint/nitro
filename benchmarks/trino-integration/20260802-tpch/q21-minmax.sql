WITH supplier_range AS (
    SELECT orderkey, min(suppkey) AS min_suppkey, max(suppkey) AS max_suppkey
    FROM "${database}"."${schema}"."${prefix}lineitem"
    GROUP BY orderkey
),
late_supplier_range AS (
    SELECT orderkey, min(suppkey) AS min_suppkey, max(suppkey) AS max_suppkey
    FROM "${database}"."${schema}"."${prefix}lineitem"
    WHERE receiptdate > commitdate
    GROUP BY orderkey
)
SELECT s.name, count(*) AS numwait
FROM "${database}"."${schema}"."${prefix}supplier" s
JOIN "${database}"."${schema}"."${prefix}lineitem" l1 ON s.suppkey = l1.suppkey
JOIN "${database}"."${schema}"."${prefix}orders" o ON o.orderkey = l1.orderkey
JOIN "${database}"."${schema}"."${prefix}nation" n ON s.nationkey = n.nationkey
JOIN supplier_range suppliers ON suppliers.orderkey = l1.orderkey
JOIN late_supplier_range late_suppliers ON late_suppliers.orderkey = l1.orderkey
WHERE o.orderstatus = 'F'
  AND l1.receiptdate > l1.commitdate
  AND suppliers.min_suppkey < suppliers.max_suppkey
  AND late_suppliers.min_suppkey = late_suppliers.max_suppkey
  AND n.name = 'SAUDI ARABIA'
GROUP BY s.name
ORDER BY numwait DESC, s.name
LIMIT 100
