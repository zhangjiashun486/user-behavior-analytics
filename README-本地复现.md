# 用户行为离线分析：Windows本地版

2026-10-13，基于josephmachado/beginner_de_project上游4b853e64复现核心数据链并改进。保留MIT许可证和UPSTREAM.json。本地首个Git提交只是上游快照，不是个人贡献。

## 已运行的核心链路

OnlineRetail.csv和movie_review.csv → Python抽样/质量检查 → PySpark 3.5.1本地Tokenizer与StopWordsRemover → good词规则分类 → PyArrow写Parquet并读回 → DuckDB按用户聚合 → CSV、HTML看板与批次来源清单。

当前样例扫描2000行购买，保留1441行有效购买、隔离559行；从27454行评论中选入2000条匹配评论，产生68个购买用户的指标。这是本次小样例的口径，不是生产规模或用户量。

业务规则：购买数量为正、价格非负、客户编号非空；退货和异常记录隔离。没有评论的购买用户保留为0。正向评论只检查good词，继承上游演示规则，没有训练情感模型。

## 真正改进

- 修正购买和评论明细多对多关联造成重复累计：先分别按客户聚合，再左关联；两笔10/20购买与三条评论构造用例中，上游金额90、评论6，修正为金额30、评论3。
- 提取dags/sql/customer_metrics.sql，Windows本地计算与原Airflow指标任务共用同一SQL。Airflow任务的源码同步了修正；完整Docker/Airflow链路已于2026-10-14跑通（8个任务全部成功），见下文与《完整部署说明-20261014.md》。
- 新增本地流水线、隔离记录、来源SHA-256、Parquet读回及可筛选的HTML指标展示。

## 运行

双击上级目录“运行数据项目.cmd”；或在本目录执行上级.venv-data/Scripts/python.exe -m local_pipeline.run --limit 2000 --batch demo-20261013。

输出：outputs/demo-20261013/dashboard.html、behaviour_metrics.csv、classified_reviews.parquet、quarantine.json、manifest.json。

核心：local_pipeline/run.py的sample_data、classify_reviews、run；local_pipeline/metrics.py的calculate_metrics；dags/sql/customer_metrics.sql。tests/metrics是金额/评论口径回归用例。

## 环境与验证边界（2026-10-13快照；最新部署状态见下文）

本机旧SPARK_HOME指向不存在的位置，本地代码只在当前进程指向项目安装的Spark，不改全局环境。Java17本地NIO失败，使用已有Corretto Java8；Spark CSV读取与ML计算在JVM执行，Parquet由PyArrow落盘。本机没有Docker/WSL，因此Postgres、MinIO、Airflow、Quarto的原完整部署未验收。没有Hive/Flink/Kafka开发成果。

Windows运行结束仍有Spark临时目录清理提示；主作业返回成功，CSV/Parquet/清单已保存并可核验。不把清理日志当作ETL成功的唯一证据。

## 责任边界

本项目为实现与验证实践：选型、环境准备与结果验收由本人确认，实现与调试过程使用 AI 编码工具辅助。项目存在、测试通过均不等于本人已能独立编写全部源码；本文的改动清单与验证记录供后续独立复现与源码复习使用。

## 2026-10-14 13:52:15 完整数据部署续接

- 来源：用户网络中断后“继续吧”；环境安装、Windows 状态核对与 Compose 检查在 AI 编码工具辅助下完成。
- 两个源码仍在<仓库目录>与beginner-de-project；新增完整部署说明见[<仓库目录>/完整部署说明-20261014.md](<仓库目录>/完整部署说明-20261014.md)。
- Docker Desktop 4.95.0已按用户模式安装；官方SHA-256与Docker Inc数字签名通过。实测Docker CLI 29.8.2、Compose v5.6.0可执行。
- WSL两项Windows系统功能已启用，Windows明确要求重启；未自行重启。官方WSL软件包/内核及Docker引擎仍需重启后核对、补齐。
- docker-compose.windows.yml与PowerShell脚本已准备，Compose配置和脚本语法检查通过；独立career-data容器组，Airflow18080、Postgres15432、MinIO19000/19001，端口仅绑定本机。原上游配置、DAG和数据未覆盖。
- **当前停点：等待用户保存工作并手动重启。** 回来后检查WSL，必要时重新运行.setup/enable-wsl.ps1安装官方WSL包，再启动Docker并构建、初始化、运行DAG及核验产物。
- **最新状态（2026-10-14）**：完整 Docker/Airflow 链路已跑通，8 个任务全部成功（购买 541,908 行 → 客户指标 4,338 行），详见《完整部署说明-20261014.md》。工程操作在 AI 编码工具辅助下完成；选型、环境准备与结果验收由本人确认，不把工具执行等同于本人已独立掌握全部实现。学习停点、总计划和招聘 Excel 未由本轮改写。
- 本次修改前副本：C:/学校/简历实习/04-历史归档/07-背景交接/20261014-135215-完整部署准备前。
