# RSP


## scala: RSP

* package: `org.szu.rsp`

### mvn生成jar包

```bash
cd rsp
mvn package
```

jar包生成在`rsp/target/rsp-1.0-SNAPSHOT.jar`

### RSPDM

#### 构造方法

```scala
class RSPDM(rspdmsPath: String, session: SparkSession)
```

|name|description|
|:-:|:-:|
|rspdmsPath|根目录|
|session|`org.apache.spark.sql.SparkSession`对象|

#### RSPDM.convert

使用RSP方法对srcPath的数据分块并保存到tarPath。

```scala
def RSPDM.convert(srcPath: String, rspNum: Int, tarPath: String, srcType: String, tarType: String): Unit
````

```scala
def RSPDM.convert(srcPath: String, rspNum: Int, tarPath: String, srcType: String): Unit
````

```scala
def RSPDM.convert(srcPath: String, rspNum: Int, tarPath: String): Unit
````


|name|description|
|:-:|:-:|
|srcPath|要分块的文件路径|
|rspNum|分块数|
|tarPath|目标文件路径|
|srcType|输入数据类型|
|tarType|输出数据类型|

* 目前支持对以下数据类型做转换，未知类型默认为text：
  * csv
  * parquet
  * json 
  * text
* 转换规则:
  * 输入srcType和tarType,会按srcType读取数据并输出成tarType。
  * 只输入srcType, 则输入和输出类型均为srcType。
  * 参数缺省则以文件后缀名判断类型。
 

### RSPDataset

#### 构造方法

```scala
// 这里util用的是java.util
class RSPDataset(rspdmsPath: String, session: SparkSession, srcType: String){
  var blockNames: util.ArrayList[String]
  var rspBlockNumber: Int
  var rspBlockLocation : util.HashMap[String, String]
}
```

```scala
class RSPDataset(rspdmsPath: String, session: SparkSession)
```

|name|description|
|:-:|:-:|
|rspdmsPath|分块文件所在路径|
|session|`org.apache.spark.sql.SparkSession`对象|
|srcType|数据文件类型，如果不输入则以后缀名判断文件类型，支持的文件类型与RSPDM一致|
|blockNames|分块名|
|rspBlockNumber|分块数|
|rspBlockLocation|分块地址映射|


```scala
def RSPDataset.getMetaData(path: String): Unit
```

扫描路径获取获取RSP数据集元信息(blockNames, rspBlockNumber, rspBlockLocation), 对象初始化的时候会直接调用。

```scala
def RSPDataset.mapF2P(partitionList: util.ArrayList[String], function: Array[Row] => Any): RDD[Any]
```

用指定的function处理选中的分块。

* partitionList: 在RSPDataset.blockNames中选取
* function: 方法，传入参数类型为元素类型为org.apache.spark.sql.Row的数组



```scala
def mapFs2P(partitionList: util.ArrayList[String], functionMap: Map[Int, Array[Row] => Any]): RDD[Any]
```

根据TaskContext.getPartitionId()获得的Id处理选择function处理选中的分块。


* partitionList: 在RSPDataSet.blockNames中选取
* functionMap:
  * Key: Int
  * Value: 方法，传入参数类型为元素类型为org.apache.spark.sql.Row的数组



### Sample

启动spark-shell：

> spark-shell --jars rsp-1.0-SNAPSHOT.jar

导入包：
> import com.example.rsp.{RSPDataSet, RSPDM} 

初始化对象
> var ds = new RSPDataSet("/mnt/d/part-00097-3",spark)

调用mapF2P, 传入方法返回分块长度
> var result = ds.mapF2P(ds.blockNames, array => array.length)

> result.collect

调用mapFs2P, 传入方法返回分块长度

> var ds.mapFs2P(ds.blockNames, Map(0 -> (array => array.length)))

> result.collect


## python: rsp

python版rsp通过调用scala版rsp运行，运行时需要确保连接的SparkSession中有导入`rsp-1.0-SNAPSHOT.jar`

本地`SparkSession`参考下面代码：


```python
from pyspark.sql import SparkSession

# JARS为rsp jar包本地路径。
JARS = "rsp-1.0-SNAPSHOT.jar" 

spark = SparkSession.builder.master("local").config("spark.jars", JARS).getOrCreate()

```

python版rsp使用方法与scala版rsp一致。


### rsp/python/rsp_wrapper.py

* rsp_wrapper.RSPDM
* rsp_wrapper.RSPDataset
