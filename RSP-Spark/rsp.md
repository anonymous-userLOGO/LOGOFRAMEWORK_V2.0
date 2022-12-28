
## 打包方法
```
进入rspv2目录
# cd ./rspv2
使用mvn编译
# mvn -T 1C  clean package -Dmaven.test.skip=true
得到jar包 ./rspv2/target/spark-rsp_2.11-2.4.0.jar
```
## 使用方法
- 将 spark-rsp_2.11-2.4.0.jar 文件 放在spark节点的当前路径,使用如下命令启动
```
spark-shell --master yarn --driver-memory 4g --num-executors 10 --jars spark-rsp_2.11-2.4.0.jar
```

- 导入上下文包
```
scala> import org.apache.spark.sql.RspContext._
import org.apache.spark.sql.RspContext._
```


- 采用rdd 原始方法读入数据(该数据共有10个文件，每个文件大小约766MB),可以看到rdd1 是一个RDD类型
```
scala> val rdd1=sc.textFile("/user/longhao/higgs_10")
rdd1: org.apache.spark.rdd.RDD[String] = /user/longhao/higgs_10 MapPartitionsRDD[1] at textFile at <console>:24
```

- 此时读入数据的块数为 60
```
scala> rdd1.getNumPartitions
res2: Int = 60
```


- 采用rspTextFile 读入数据(该数据共有10个文件，每个文件大小约766MB),可以看到 rdd2 是一个RspRDD类型
```
scala> val rdd2=sc.rspTextFile("/user/longhao/higgs_10")
rdd2: org.apache.spark.rsp.RspRDD[String] = RspRDD[6] at RDD at RspRDD.scala:11
```

- 此时读入数据的块数为 10
```
scala> rdd2.getNumPartitions
res4: Int = 10
```

- 对rdd进行RSP转换，该算子参数为目标rsp块数目
```
scala> val rdd3=rdd1.toRSP(100)
rdd3: org.apache.spark.rsp.RspRDD[String] = RspRDD[10] at RDD at RspRDD.scala:11

scala> rdd3.getNumPartitions
res5: Int = 100
```
- 对RspRDD 取若干个块,如下对rdd2取2个数据块，生成的rdd4仍然是RspRDD
```
scala> val rdd4=rdd2.getSubPartitions(2)
rdd4: org.apache.spark.rsp.RspRDD[String] = SonRDD[11] at RDD at RspRDD.scala:11
```




- 对RspRDD 进行rspCollect操作，该算子接受需collect的块数量，默认为1,该操作返回的数据量依然不能很大，不然会报错
```
scala> val data=rdd2.rspCollect(1)
[Stage 0:>                                                          (0 + 1) / 1]
```

- dataframe格式下，rspRead支持大部分的格式导入
```
scala> spark.rspRead.   #后面按table键盘
csv   format   jdbc   json   load   option   options   orc   parquet   schema   table   text   textFile
```


- 采用原始Dataset方式读入csv数据
```
scala> val df1=spark.read.csv("/user/longhao/higgs_500")
20/12/01 15:00:22 WARN lineage.LineageWriter: Lineage directory /var/log/spark/lineage doesn't exist or is not writable. Lineage for this application will be disabled.
[Stage 0:==========================================>            (389 + 6) / 500]
df1: org.apache.spark.sql.DataFrame = [_c0: string, _c1: string ... 27 more fields]

scala> df1.rdd
res3: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[15] at rdd at <console>:29

#对应rdd的partition数量被优化整理过
scala> df1.rdd.getNumPartitions
res0: Int = 125
```

- 采用rsp的方式读入csv数据，生成的是RspDataset
```
scala> val df2=spark.rspRead.csv("/user/longhao/higgs_500")
20/12/01 15:02:05 WARN lineage.LineageWriter: Lineage directory /var/log/spark/lineage doesn't exist or is not writable. Lineage for this application will be disabled.
[Stage 2:================================================>     (452 + 48) / 500]
df2: org.apache.spark.sql.RspDataset[org.apache.spark.sql.Row] = [_c0: string, _c1: string ... 27 more fields]

# 它的rdd也是RspRDD
scala> df2.rdd
res2: org.apache.spark.rsp.RspRDD[org.apache.spark.sql.Row] = RspRDD[35] at RDD at RspRDD.scala:11

#对应rdd的partition数量等同于该目录下文件的数量
scala> df2.rdd.getNumPartitions
res1: Int = 500

```

- parquet格式读入,该路径下共有80个parquet文件
```
scala> val df3=spark.rspRead.parquet("/user/longhao/rsp/badvoice/zc_calling_detial_unicom_bdr_hour")
20/12/01 15:08:52 WARN lineage.LineageWriter: Lineage directory /var/log/spark/lineage doesn't exist or is not writable. Lineage for this application will be disabled.
df3: org.apache.spark.sql.RspDataset[org.apache.spark.sql.Row] = [record_type: string, roam_city_code: int ... 30 more fields]

#路径中，每个parquret文件对应一个partition
scala> df3.rdd.getNumPartitions
res5: Int = 80
```
- RspDataset 目前支持三个rsp相关算子，分别为rspCollect、rspDescribe、getSubDataset
他们均接受无参数，或者有一个参数，其值为，目标块数目

- RspRDD rspMapPartitions 算子，返回值为RDD
```
#导入rsp数据块
scala> val rdd1=sc.rspTextFile("/user/longhao/higgs_10")
rdd1: org.apache.spark.rsp.RspRDD[String] = RspRDD[5] at RDD at RspRDD.scala:11

#定义测试函数
scala> def test(iter: Iterator[String]): Iterator[Int]={ var count:Int=0; while(iter.hasNext){count=count+1;val cur=iter.next}; Array(count).toIterator}
test: (iter: Iterator[String])Iterator[Int]

#原生mapPartitions算子
scala> val rdd2=rdd1.mapPartitions(test)
rdd2: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[9] at mapPartitions at <console>:30

scala> rdd2.collect()
res2: Array[Int] = Array(1100119, 1101710, 1099389, 1099937, 1100109, 1098972, 1100463, 1099691, 1099975, 1099635)

#测试rspMapPartitions算子
scala> val rdd2=rdd1.rspMapPartitions(2,test)
rdd2: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[11] at rspMapPartitions at <console>:30

scala> rdd2.collect()
res3: Array[Int] = Array(1100119, 1101710)
```
- RspDataset rspMapPartitions 算子，返回值为RDD,底层实际是调用对应RspRDD的算子
```
scala> import org.apache.spark.sql.Row
import org.apache.spark.sql.Row

scala> def test(iter: Iterator[Row]): Iterator[Int]={ var count:Int=0; while(iter.hasNext){count=count+1;val cur=iter.next}; Array(count).toIterator}
test: (iter: Iterator[org.apache.spark.sql.Row])Iterator[Int]

scala> val result=df2.rspMapPartitions(2,test)
result: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[30] at rspMapPartitions at <console>:31

scala> result.collect()
res4: Array[Int] = Array(1101710, 1100463)
```