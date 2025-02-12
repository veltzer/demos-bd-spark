EXPLAIN
SELECT c.segment, COUNT(*) as order_count
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.segment;
EXPLAIN SELECT o.order_id FROM orders o JOIN orders o2 ON o.customer_id = o2.customer_id+1;
EXPLAIN
SELECT /*+ BROADCAST(c) */ c.segment, COUNT(*) as order_count
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.segment;
