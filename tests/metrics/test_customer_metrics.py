from decimal import Decimal
from local_pipeline.metrics import calculate_metrics

def test_many_to_many_join_does_not_multiply_amount_or_reviews():
    purchases=[('A',1,'10'),('A',1,'20')]
    reviews=[('A',True),('A',False),('A',True)]
    original=calculate_metrics(purchases,reviews,original=True)[0]
    corrected=calculate_metrics(purchases,reviews)[0]
    assert original['amount_spent']==Decimal('90')
    assert original['num_reviews']==6
    assert corrected=={'customer_id':'A','amount_spent':Decimal('30'),'num_positive_reviews':2,'num_reviews':3}

def test_purchaser_without_review_is_retained_with_zero_counts():
    assert calculate_metrics([('A',2,'5')],[])==[{'customer_id':'A','amount_spent':Decimal('10'),'num_positive_reviews':0,'num_reviews':0}]

def test_reviews_without_purchases_are_excluded_from_purchase_report():
    assert calculate_metrics([],[('A',True)])==[]

def test_two_customers_are_not_merged():
    rows=calculate_metrics([('A',1,'10'),('B',3,'2')],[('A',True),('B',False)])
    assert rows[0]['amount_spent']==10 and rows[1]['amount_spent']==6
    assert rows[0]['num_positive_reviews']==1 and rows[1]['num_positive_reviews']==0

def test_decimal_amount_is_exact_and_repeated_runs_agree():
    purchases=[('A',3,'0.10'),('A',1,'0.20')]
    assert calculate_metrics(purchases,[])==calculate_metrics(purchases,[])
    assert calculate_metrics(purchases,[])[0]['amount_spent']==Decimal('0.50')

def test_raw_purchase_quality_rules_exclude_invalid_entities_and_returns():
    purchases=[('A',1,'10'),('A',-2,'10'),('B',0,'3'),('C',1,'-2'),(None,1,'20'),('FREE',1,'0')]
    reviews=[('A',True),('A',False),(None,True)]
    assert calculate_metrics(purchases,reviews)==[
        {'customer_id':'A','amount_spent':Decimal('10'),'num_positive_reviews':1,'num_reviews':2},
        {'customer_id':'FREE','amount_spent':Decimal('0'),'num_positive_reviews':0,'num_reviews':0},
    ]
