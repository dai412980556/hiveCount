package Utils

import bean.DataSourceInfo
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.slf4j.{Logger, LoggerFactory}
import spark.SQLContextProvider._


object SparkMLUtils {

    /**
      * 日志工具类
      */
    val logger: Logger = LoggerFactory.getLogger(SparkMLUtils.getClass)

    /**
      * 加载数据集
      *
      * @param resourcePath 数据源路径
      * @return DatSet数据集
      */
    def loadDataset(resourcePath: String, columns: String, principal: String, keytab: String): DataFrame = {
        loadResourceDF(resourcePath, columns, principal, keytab)
    }

    /**
      * 分类加载各种数据源
      *
      * @param resourcePath 资源路径
      * @return DataFrame 数据集
      */
    def loadResourceDF(resourcePath: String, columns: String, principal: String, keytab: String): DataFrame = {

        if (CommonUtil.isJsonObject(resourcePath)) {
            val dataSourceInfo = CommonUtil.gson.fromJson(resourcePath, classOf[DataSourceInfo])
            println("=============================转换成的对象是：" + dataSourceInfo.toString)
            if ("Hive".equals(dataSourceInfo.dbType)) loadHiveFile(dataSourceInfo, columns) else loadHBaseData(dataSourceInfo, columns, principal, keytab)
        } else {
            loadParquetFromResources(resourcePath)
        }
    }

    /**
      * 加载parquet格式的数据
      *
      * @param resourcePath 资源路径
      * @param sqlContext   Spark上下文对象
      * @return DataFrame 读取的数据集
      */
    def loadParquetFromResources(resourcePath: String)(implicit sqlContext: SQLContext): DataFrame = {
        sqlContext.read.parquet(resourcePath)
    }


    /**
      * Spark读取Hive表数据
      *
      * @param dataSourceInfo 数据源：hive库名和表名
      * @param hiveContext    hive上下文
      * @return 读取的数据集
      */
    def loadHiveFile(dataSourceInfo: DataSourceInfo, columns: String)(implicit hiveContext: HiveContext): DataFrame = {

        if (CommonUtil.isEmpty(columns)) {
            scanHiveTable()
        } else {
            val tableName = dataSourceInfo.dbName + "." + dataSourceInfo.tableName
            logger.info(s"加载的表名为：$tableName")
            val data = hiveContext.sql(s"select $columns from $tableName")
            data
        }
    }

    /**
      * 扫描Hive中所有的库和表
      */
    def scanHiveTable(): DataFrame = {
        logger.info("开始扫描Hive表......")
        val databaseDF = hiveContext.sql("show databases").limit(3)
        //拿到所有数据库
        val databases = databaseDF.map(row => row.mkString).toArray()
        //拿到所有表信息
        val allTables = databases.map(instance => {
            hiveContext.sql(s"use $instance")
            val tableDF = hiveContext.sql("show tables").limit(3)
            val tables = tableDF.map(row => (instance, row(0).toString))
            tables.collect()
        }).reduce((first, second) => first ++ second)
        //统计所有表的行数
        val dataSet = allTables.map(item => {
            val lineNumber = hiveContext.sql(s"select count(1) from ${item._1 + "." + item._2}")
                    .map(row => row.mkString).collect()(0)
            (item._1, item._2, lineNumber)
        })
        //将结果封装成DataFrame类型
        val resultData = sqlContext.createDataFrame(dataSet).toDF("database", "table", "rows")
        //返回结果数据
        resultData
    }

    /**
      * Spark读取本地ORC文件
      *
      * @param resourcePath 资源路径
      * @param sqlContext   SparkSQL上下文
      * @return 读取的数据集
      */
    def loadORCFile(resourcePath: String)(implicit sqlContext: SQLContext): DataFrame = {
        sqlContext.read.orc(resourcePath)
    }

    /**
      * Spark读取CSV格式数据
      *
      * @param absolutePath 数据源路径
      * @param sqlContext   SparkSQL上下文对象
      * @return 读取的数据集
      */
    def loadCSVFromResources(absolutePath: String)(implicit sqlContext: SQLContext): DataFrame = {
        sqlContext.read.format("csv")
                .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
                .option("delimiter", ",") //切分符号
                .option("inferSchema", "true") //这是自动推断属性列的数据类型
                .load(absolutePath)
    }

    /**
      * Spark读取HBase数据源
      *
      * @param dataSourceInfo 数据源路径
      * @param sqlContext     SparkSQL上下文对象
      * @return 读取的数据集
      */
    def loadHBaseData(dataSourceInfo: DataSourceInfo, columns: String, principal: String, keytab: String)(implicit sqlContext: SQLContext): DataFrame = {
        //获取HBase表中的数据
        val resultSet = if ("*".equals(columns)) HBaseUtil.getAllColumns(dataSourceInfo, principal, keytab) else HBaseUtil.scanTable(dataSourceInfo, columns, principal, keytab)
        //将输入格式不同的列进行统一
        val integrateColumn = if ("*".equals(columns)) {
            val allColumns = sqlContext.sparkContext.parallelize(HBaseUtil.columnNames)
                    .reduceByKey((first, second) => first + ":" + second)
                    .map(x => x._1 + ":" + x._2)
                    .toLocalIterator
                    .mkString(",").trim
            allColumns
        } else columns
        //将结果包装成RDD格式
        val hbaseRDD = sqlContext.sparkContext.makeRDD(resultSet)
        //将数据映射为表,也就是将RDD转化为dataframe schema
        val dataFrame = hbaseRDD.map(r => Row.fromSeq(r))
        //拿到所有列名
        val columnNames = integrateColumn.split(",") flatMap (fields => fields.split(":").drop(1))
        //创建一个表的schema
        val schema = StructType(columnNames.map(StructField(_, StringType)))
        //使用sqlContext创建一个DataFrame
        val readData = sqlContext.createDataFrame(dataFrame, schema)
        //返回结果
        readData
    }
}

/**
  * 将属性和标签封装成LabelPoint
  *
  * @param label    标签
  * @param features 属性数组
  */
case class LabeledFeatures(label: Double, features: Array[Double]) {
    def toLabeledPoints = LabeledPoint(label, Vectors.dense(features))
}

/**
  * 将标签和属性向量组装成LabelPoint
  *
  * @param label    标签
  * @param features 属性向量
  */
case class LabeledVector(label: Double, features: org.apache.spark.mllib.linalg.Vector) {
    def toLibLabeledPoints = LabeledPoint(label, features)
}

/**
  * 没有标签的属性向量
  *
  * @param id       给定的序号ID
  * @param features 属性向量
  */
case class UnlabeledVector(id: Double, features: org.apache.spark.mllib.linalg.Vector)
