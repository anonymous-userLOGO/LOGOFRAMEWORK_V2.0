# LOGO_FRAMEWORK_2.0.0

## 介绍
LOGO是一个分布式并行计算框架。

## 部署

1. [配置LOGO使用环境](https://github.com/anonymous-userLOGO/LOGOFRAMEWORK_V2.0/blob/master/RSP-Spark/README_Spark%20with%20RSP1.0.md)

   如这一步配置失败，可在提交任务时指定RSP的[jar包](https://github.com/anonymous-userLOGO/LOGOFRAMEWORK_V2.0/blob/master/LOGO_FRAMEWORK/dependencies/spark-rsp_2.11-2.4.0.jar)以解决依赖问题。

2. 安装Smile算法库

   Smile算法库的安装具体参考[Smile官方Github](https://github.com/haifengl/smile)。

3. 使用LOGO

   通过Maven打包[LOGO项目](https://github.com/anonymous-userLOGO/LOGOFRAMEWORK_V2.0/tree/master/LOGO_FRAMEWORK)，通过`spark-submit`提交任务，指定入口类为`com.szubd.rspalgos.App`，参数包括算法参数以及集群资源参数。

   参考命令：

   ```shell
   $SPARK_HOME/bin/spark-submit \
   --master yarn \
   --deploy-mode cluster \
   --class com.szubd.rspalgos.App \
   --jars $root/dependencies/spark-rsp_2.11-2.4.0.jar \
   --conf spark.yarn.maxAppAttempts=0 \
   --conf spark.task.maxFailures=1 \
   --name $appName \
   $algojar \
   clf logoShuffle DT dataset.parquet 0.05 1 0.05 8 1000
   ```
## 许可

采用 Apache-2.0 开源协议，细节请阅读[LICENSE](https://github.com/anonymous-userLOGO/LOGOFRAMEWORK_V2.0/blob/master/LICENSE)文件。
