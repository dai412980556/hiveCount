package spark
import Utils.PropertyUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}


trait SQLContextProvider {
  /**
    * 日志工具类
    */
  val logger: Logger = LoggerFactory.getLogger(SQLContextProvider.getClass)
  /**
    * 创建SQLContext对象
    */
  implicit val sqlContext: SQLContext = SQLContextProvider.sqlContext
  /**
    * 创建SparkContext对象
    *
    */
  implicit val sparkContext: SparkContext = SQLContextProvider.sparkContext
  /**
    * 创建HiveContext
    */
  implicit val hiveContext: HiveContext = SQLContextProvider.hiveContext
  /**
    * 输出日志显示得到的数据数量
    */
  logger.trace("Numbers of cached RDDs:" + sqlContext.sparkContext.getPersistentRDDs.size)

}

/**
  * 生成单例模式的SparkSession对象
  */
object SQLContextProvider {
  //判断环境是生产还是开发
  val conf: SparkConf = if ("dev".equals(PropertyUtil.getProperty("spark.env")))
    new SparkConf().setMaster("local[*]").setAppName("AlgorithmProgram")
  else new SparkConf().setAppName("AlgorithmProgram")
  //SparkContext
  implicit val sparkContext: SparkContext = new SparkContext(conf)
  //SQLContext
  implicit val sqlContext: SQLContext = new SQLContext(sparkContext)
  //HiveContext
  implicit val hiveContext: HiveContext = new HiveContext(sparkContext)

  sparkContext.setLogLevel("ERROR")


}
