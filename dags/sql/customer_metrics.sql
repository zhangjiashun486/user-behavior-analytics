WITH purchase_totals AS (
  SELECT customer_id, SUM(quantity * unit_price) AS amount_spent
  FROM purchases
  WHERE customer_id IS NOT NULL AND quantity > 0 AND unit_price >= 0
  GROUP BY customer_id
), review_totals AS (
  SELECT cid, COUNT(*) AS num_reviews,
    SUM(CASE WHEN positive_review THEN 1 ELSE 0 END) AS num_positive_reviews
  FROM reviews
  WHERE cid IS NOT NULL
  GROUP BY cid
)
SELECT p.customer_id, p.amount_spent,
  COALESCE(r.num_positive_reviews, 0) AS num_positive_reviews,
  COALESCE(r.num_reviews, 0) AS num_reviews
FROM purchase_totals p
LEFT JOIN review_totals r ON p.customer_id = r.cid
ORDER BY p.customer_id
