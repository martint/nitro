SELECT avg(ss_ext_discount_amt)
FROM ${database}.${schema}.store_sales
WHERE ss_quantity BETWEEN 1 AND 20
