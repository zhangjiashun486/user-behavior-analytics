from pathlib import Path
from datetime import datetime,timezone
from decimal import Decimal,InvalidOperation
from html import escape
import argparse,csv,hashlib,json,os,sys,time
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer,StopWordsRemover
from pyspark.sql.functions import array_contains
from .metrics import calculate_metrics

ROOT=Path(__file__).resolve().parents[1]

def sample_data(limit):
    purchases=[];rejected=[];seen=0
    with (ROOT/'data/OnlineRetail.csv').open(encoding='utf-8-sig',newline='') as f:
        for row in csv.DictReader(f):
            seen+=1
            try:
                cid=row['CustomerID'].strip();quantity=int(row['Quantity']);price=Decimal(row['UnitPrice'])
                if not cid:raise ValueError('missing_customer_id')
                if quantity<=0:raise ValueError('return_or_nonpositive_quantity')
                if not price.is_finite() or price<0:raise ValueError('invalid_price')
                purchases.append((cid,quantity,str(price)))
            except (ValueError,InvalidOperation) as e:
                rejected.append({'row_number':seen+1,'reason':str(e)})
            if seen>=limit:break
    customers={r[0] for r in purchases};reviews=[];review_scanned=0
    with (ROOT/'data/movie_review.csv').open(encoding='utf-8-sig',newline='') as f:
        for row in csv.DictReader(f):
            review_scanned+=1
            if row['cid'] in customers and row['review_str'].strip():reviews.append((row['cid'],row['review_str']))
            if len(reviews)>=limit:break
    return purchases,reviews,rejected,seen,review_scanned

def classify_reviews(input_csv):
    import pyspark
    # Isolate this project's installed Spark from a stale machine-wide SPARK_HOME.
    os.environ['SPARK_HOME']=str(Path(pyspark.__file__).resolve().parent)
    local_java=os.environ.get('LOCAL_SPARK_JAVA_HOME')
    compatible_java=Path.home()/'.jdks/corretto-1.8.0_482'
    if local_java:os.environ['JAVA_HOME']=local_java
    elif os.name=='nt' and compatible_java.exists():os.environ['JAVA_HOME']=str(compatible_java)
    os.environ['SPARK_LOCAL_HOSTNAME']='localhost'
    os.environ['PYSPARK_PYTHON']=sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON']=sys.executable
    spark=(SparkSession.builder.master('local[1]').appName('career-user-analytics')
           .config('spark.driver.memory','512m').config('spark.sql.shuffle.partitions','1')
           .config('spark.ui.enabled','false').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    try:
        # Read through Spark's JVM CSV reader, avoiding a Python worker on
        # Windows for transformations that are already implemented in JVM ML.
        raw=(spark.read.schema('cid string, review_str string').option('header',True)
             .option('multiLine',True).option('escape','"').csv(input_csv.as_uri()))
        tokens=Tokenizer(inputCol='review_str',outputCol='tokens').transform(raw)
        clean=StopWordsRemover(inputCol='tokens',outputCol='clean').transform(tokens)
        result=clean.select('cid',array_contains('clean','good').alias('positive_review'))
        return [(r.cid,r.positive_review) for r in result.collect()]
    finally:spark.stop()

def write_dashboard(metrics,dest,manifest):
    rows=''.join('<tr><td>'+escape(r['customer_id'])+'</td><td>'+str(r['amount_spent'])+'</td><td>'+str(r['num_reviews'])+'</td><td>'+str(r['num_positive_reviews'])+'</td></tr>' for r in metrics)
    bars=''.join('<div class="barrow"><span>'+escape(r['customer_id'])+'</span><meter value="'+str(r['amount_spent'])+'" max="'+str(max((float(x['amount_spent']) for x in metrics),default=1))+'"></meter><span>'+str(r['amount_spent'])+'</span></div>' for r in sorted(metrics,key=lambda r:r['amount_spent'],reverse=True)[:10])
    page='''<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>用户行为离线分析</title><style>body{font:15px/1.6 system-ui;max-width:1000px;margin:40px auto;padding:0 24px;color:#193340}h1{margin-bottom:0}.muted{color:#607680}.stats{display:flex;gap:32px;margin:25px 0}.stats b{font-size:26px;display:block}table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:8px;border-bottom:1px solid #dbe6eb}.barrow{display:grid;grid-template-columns:80px 1fr 120px;gap:12px;align-items:center}meter{width:100%}input{padding:8px;margin-bottom:12px;border:1px solid #aebfc7;border-radius:4px}</style><h1>用户行为离线分析</h1><p class="muted">公开零售与评论数据小规模复现 · PySpark规则文本处理 · DuckDB指标计算</p>'''
    page+='<div class="stats"><div><b>'+str(manifest['valid_purchase_rows'])+'</b>有效购买行</div><div><b>'+str(manifest['review_rows'])+'</b>评论行</div><div><b>'+str(len(metrics))+'</b>购买用户</div><div><b>'+str(manifest['quarantined_rows'])+'</b>隔离行</div></div>'
    page+='<p>口径：正数量、非负价格的购买记录；退货/无客户编号等记录隔离。按用户分别聚合买入与评论，再左关联；无评论用户保留为0。情感规则仅检测单词good，没有训练模型。</p><h2>购买金额前10用户</h2>'+bars
    page+='<h2>用户指标</h2><input id="filter" placeholder="筛选客户编号"><table><thead><tr><th>客户</th><th>购买金额</th><th>评论数</th><th>规则正向评论数</th></tr></thead><tbody>'+rows+'</tbody></table><script>document.querySelector("#filter").oninput=e=>{for(const r of document.querySelectorAll("tbody tr"))r.hidden=!r.cells[0].textContent.includes(e.target.value)}</script></html>'
    dest.write_text(page,encoding='utf-8')

def run(limit=5000,batch='demo'):
    if not batch or not all(c.isalnum() or c in '-_' for c in batch):raise ValueError('invalid batch id')
    out=ROOT/'outputs'/batch;out.mkdir(parents=True,exist_ok=True)
    started=time.perf_counter()
    purchases,reviews,rejected,seen,scanned=sample_data(limit)
    input_csv=out/'sampled_reviews.csv'
    with input_csv.open('w',encoding='utf-8',newline='') as f:
        writer=csv.writer(f);writer.writerow(['cid','review_str']);writer.writerows(reviews)
    classified=classify_reviews(input_csv)
    pq.write_table(pa.Table.from_pylist([{'cid':c,'positive_review':p,'batch_id':batch} for c,p in classified]),out/'classified_reviews.parquet')
    # Read the actual saved Parquet artefact before calculating downstream metrics.
    stored=pq.read_table(out/'classified_reviews.parquet').to_pylist()
    metrics=calculate_metrics(purchases,[(r['cid'],r['positive_review']) for r in stored])
    with (out/'behaviour_metrics.csv').open('w',encoding='utf-8',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=['customer_id','amount_spent','num_positive_reviews','num_reviews']);writer.writeheader();writer.writerows(metrics)
    (out/'quarantine.json').write_text(json.dumps(rejected,indent=2),encoding='utf-8')
    manifest={'batch_id':batch,'created_at':datetime.now(timezone.utc).isoformat(),'limit':limit,'purchase_rows_scanned':seen,'valid_purchase_rows':len(purchases),'review_rows_scanned':scanned,'review_rows':len(reviews),'customer_metrics':len(metrics),'quarantined_rows':len(rejected),'seconds':round(time.perf_counter()-started,3),'sources':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [ROOT/'data/OnlineRetail.csv',ROOT/'data/movie_review.csv']},'scope':'Windows local adapter; Docker/Airflow/Postgres/MinIO not executed','spark':'3.5.1 local[1]','classification':'upstream tokenization/stopwords/good-word rule, not trained sentiment model'}
    (out/'manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
    write_dashboard(metrics,out/'dashboard.html',manifest)
    return manifest

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--limit',type=int,default=5000);parser.add_argument('--batch',default='demo')
    args=parser.parse_args();print(json.dumps(run(args.limit,args.batch),ensure_ascii=False,indent=2))
