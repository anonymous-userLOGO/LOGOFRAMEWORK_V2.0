# Project scenario：
usage：
```powershell
$SPARK_HOME/bin/pyspark --master yarn --rsp-program true --executor-cores 4 --executor-memory 16G --num-executors 6
```
**启动spark-shell、spark-submit同理；**

启动时**必须**指定新参数的值（true/false）      若输入true即可以rsp-program方式启动；

查看帮助文档提示。

```bash
--rsp-program               Start with rsp mode
```

# Result：

```bash
luokaijing@ares02:~/RSP-0.1$ $SPARK_HOME/bin/pyspark --master yarn --rsp-program true --executor-cores 4 --executor-memory 16G --num-executors 6
Python 3.7.4 (default, Aug 13 2019, 20:35:49)
[GCC 7.3.0] :: Anaconda, Inc. on linux
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
20/11/02 19:30:34 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
20/11/02 19:30:35 WARN yarn.Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.4.0 with RSP1.0
      /_/

Using Python version 3.7.4 (default, Aug 13 2019 20:35:49)
SparkSession available as 'spark'.
Running pyspark with RSP1.0 mode!
>>> # 导包
...
>>> from time import *
>>> import socket
import os
>>> import os
```



# expected effect（done）：
0.实现了添加运行参数--rsp-program，并给每种启动方式打上“RSP” logo。
1.集群中每个节点都有executor，且能顺利运行结束。
2.集群中每个节点都只起一个executor，可动态获取每个节点的名称。
3.正常来说不会出现失败或ANY状态。
4.能顺利从sparkcontext或sparkconf中获取参数信息。




