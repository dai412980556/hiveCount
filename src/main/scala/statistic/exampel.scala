package statistic

import java.security.PrivilegedExceptionAction
import java.text.SimpleDateFormat
import java.util.Calendar

import Utils.{CommonUtil, HDFSUtil, SparkMLUtils}
import bean.DataSourceInfo
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser
import spark.SQLContextProvider

/**
  * 网省新增用户数
  */
object exampel {
  val logger: Logger = LoggerFactory.getLogger(exampel.getClass)
    /** 命令行参数 */
    case class Params(input_pt: String = "",
                      begin_date : String = "",
                      end_date : String = "",
                      output_pt: String = "",
                      principal: String = "",
                      keytab: String = ""
                     )

    def main(args: Array[String]): Unit = {
        /**
          * 默认参数
          */
        val default_params = Params()
        /**
          * 进行参数的解析
          */
        val parser = new OptionParser[Params](getClass.getName) {
            head(s"${getClass.getName}: statistical analysis class.")
            opt[String]("input_pt")
                    .required()
                    .text("输入文件路径")
                    .action((x, c) => c.copy(input_pt = x))
            opt[String]("output_pt")
                    .text("输出文件路径")
                    .action((x, c) => c.copy(output_pt = x))
            opt[String]("principal")
                    .text("Kerberos认证用户")
                    .action((x, c) => c.copy(principal = x))
            opt[String]("keytab")
                    .text("Kerberos用户的keytab")
                    .action((x, c) => c.copy(keytab = x))
            opt[String]("end_date")
              .text("截至时间")
              .action((x, c) => c.copy(end_date = x))
            opt[String]("begin_date")
              .text("开始时间")
              .action((x, c) => c.copy(begin_date = x))
        }

        /**
          * 解析参数
          */
        parser.parse(args, default_params) match {
            case Some(params) => run(params)
            case _ => throw new IllegalArgumentException("传入参数有误")
        }

    }

    /**
      * 执行算法
      *
      * @param p 命令行传入的参数
      */
    def run(p: Params): Unit = {
        //没有Kerberos
        if (CommonUtil.isEmpty(p.principal) || CommonUtil.isEmpty(p.keytab)) {
            //SparkContext
            val sparkContext: SparkContext = SQLContextProvider.sparkContext
            //SQLContext
            val sqlContext: SQLContext = SQLContextProvider.sqlContext
            //HiveContext
            val hiveContext: HiveContext = SQLContextProvider.hiveContext
            //加载数据源配置文件
            val dataSourceConfig = if (HDFSUtil.isDir(FileSystem.get(sparkContext.hadoopConfiguration), p.input_pt))
                sparkContext.makeRDD(Array(p.input_pt))
            else sparkContext.textFile(p.input_pt)
            //加载原始数据
            val originalData = SparkMLUtils.loadDataset(dataSourceConfig.toLocalIterator.mkString, null, p.principal, p.keytab)
            //保存结果
            if (!CommonUtil.isEmpty(p.output_pt)) {
                originalData.write.format("parquet") //以parquet格式来写入数据
                        .mode("overwrite") //覆盖以前数据
                        .save(p.output_pt)
            }
            //spark上下文
            sparkContext.stop()

        } else {
            //设置kerberos权限
            System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
            UserGroupInformation.loginUserFromKeytab(p.principal, p.keytab)
            UserGroupInformation.getLoginUser.doAs( new PrivilegedExceptionAction[Unit] {
              def run: Unit = {
                //SparkContext
                val sparkContext: SparkContext = SQLContextProvider.sparkContext
                //SQLContext
                val sqlContext: SQLContext = SQLContextProvider.sqlContext
                //HiveContext
                val hiveContext: HiveContext = SQLContextProvider.hiveContext
                //拿到fileSystem对象
                val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
                //hive仓库地址
                val hiveWareHouse = s"/user/hive/warehouse/${p.keytab.split("/").takeRight(1)(0).dropRight(7)}"
               // val hiveWareHouse = "/user/hive/warehouse"
                //                    val kerberosUsers = HDFSUtil.getNextList(fileSystem, hiveWareHouse)
                ///${p.keytab.dropRight(7)}
                //加载数据源配置文件
                val dataSourceConfig = if (HDFSUtil.isDir(FileSystem.get(sparkContext.hadoopConfiguration), p.input_pt))
                  sparkContext.makeRDD(Array(p.input_pt))
                else sparkContext.textFile(p.input_pt)
                //val dd = originalData.na.drop("all",Seq("hfsdsj","jddjsj"))
                val dataSourceInfo = CommonUtil.gson.fromJson(dataSourceConfig.toLocalIterator.mkString, classOf[DataSourceInfo])
                hiveContext.sql(s"use ${dataSourceInfo.dbName}")

                //执行查询方法""
                val originalData = hiveContext.sql(" select count(1) AS XZYHS from DWD_CST_CONSUMER WHERE BUILDDATE >= '"+p.begin_date+"' AND BUILDDATE < '"+p.end_date+"' AND (SGSTATUS <> '9' OR (SGSTATUS = '9' AND CANCELDATE >= '"+p.end_date+"'))");
                originalData.show(true)
                if (!CommonUtil.isEmpty(p.output_pt)) {
                  originalData.write.format("parquet") //以parquet格式来写入数据
                    .mode("overwrite") //覆盖以前数据
                    .save(p.output_pt)
                }
                //spark上下文
                sparkContext.stop()
              }
            })
        }
    }
    def getNowMonthStart():String={
      var period:String=""
      var cal:Calendar =Calendar.getInstance();
      var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
      cal.set(Calendar.DATE, 1)
      period=df.format(cal.getTime())//本月第一天
      period
    }
    def getNowMonthEnd():String={
      var period:String=""
      var cal:Calendar =Calendar.getInstance();
      var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
      cal.set(Calendar.DATE, 1)
      cal.roll(Calendar.DATE,-1)
      period=df.format(cal.getTime())//本月最后一天
      period
    }
}