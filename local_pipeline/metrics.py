import duckdb
from pathlib import Path

ORIGINAL_SQL='''
SELECT p.customer_id,SUM(p.quantity*p.unit_price) amount_spent,
SUM(CASE WHEN r.positive_review THEN 1 ELSE 0 END) num_positive_reviews,
COUNT(r.cid) num_reviews
FROM purchases p JOIN reviews r ON p.customer_id=r.cid
GROUP BY p.customer_id ORDER BY p.customer_id
'''

# Aggregate both sides at customer grain before joining. Keep purchasing
# customers without reviews, with an explicit zero-review metric.
CORRECTED_SQL=(Path(__file__).resolve().parents[1]/'dags/sql/customer_metrics.sql').read_text(encoding='utf-8')

def calculate_metrics(purchases,reviews,original=False):
    with duckdb.connect() as conn:
        conn.execute('CREATE TABLE purchases(customer_id VARCHAR,quantity BIGINT,unit_price DECIMAL(18,4))')
        conn.execute('CREATE TABLE reviews(cid VARCHAR,positive_review BOOLEAN)')
        if purchases:conn.executemany('INSERT INTO purchases VALUES (?,?,?)',purchases)
        if reviews:conn.executemany('INSERT INTO reviews VALUES (?,?)',reviews)
        cursor=conn.execute(ORIGINAL_SQL if original else CORRECTED_SQL)
        names=[x[0] for x in cursor.description]
        return [dict(zip(names,row)) for row in cursor.fetchall()]
