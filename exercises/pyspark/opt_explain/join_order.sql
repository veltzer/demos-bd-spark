EXPLAIN COST
SELECT /*+ LEADING(o, c, p) */
    c.country, p.category, SUM(o.amount)
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
GROUP BY c.country, p.category;
