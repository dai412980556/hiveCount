package Utils

import bean.DataSourceInfo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName, TableNotFoundException}
import org.apache.hadoop.security.UserGroupInformation
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * HBase工具类
  */
object HBaseUtil {
    /**
      * 日志工具类
      */
    @transient lazy val logger: Logger = LoggerFactory.getLogger(HBaseUtil.getClass)

    /**
      * 获取全量数据时给拿到HBase列名
      */
    var columnNames: Array[(String, String)] = _

    /**
      * HBase配置信息
      */
    var hbaseConf: Configuration = _
    /**
      * HBase连接对象
      */
    var hbaseConnection: Connection = _

    /**
      * 获取hbase配置
      *
      * @return HBase配置信息
      */
    def initHBaseConnection(dataSourceInfo: DataSourceInfo, principal: String, keytab: String): Unit = {
        try {
            //获取hbase的conf
            hbaseConf = HBaseConfiguration.create()
            //Kerberos安全验证
            hbaseConf.set("hadoop.security.authentication", "Kerberos")
            hbaseConf.set("hbase.security.authentication", "kerberos")
            //设置Hbase的各个配置参数
            hbaseConf.set("hbase.zookeeper.quorum", dataSourceInfo.ip)
            hbaseConf.set("hbase.zookeeper.property.clientPort", dataSourceInfo.port.toString)
            hbaseConf.set("zookeeper.znode.parent", "/hbase")
            UserGroupInformation.setConfiguration(hbaseConf)
            UserGroupInformation.loginUserFromKeytab(principal, keytab)
            hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
        } catch {
            case e: Exception => logger.error(">>>连接hbase失败:" + e)
        }
    }

    /**
      * 扫描HBase表,拿到所选列簇和列
      *
      * @param dataSourceInfo 数据源信息
      * @param columns        列簇名和列名
      * @param principal      认证的用户
      * @param keytab         认证用户的keytab
      * @return 查询的结果集
      */
    def scanTable(dataSourceInfo: DataSourceInfo, columns: String, principal: String, keytab: String): ArrayBuffer[Array[String]] = {
        //结果数据集数组
        val resultSet: ArrayBuffer[Array[String]] = ArrayBuffer()
        try {
            initHBaseConnection(dataSourceInfo, principal, keytab)
            val admin = hbaseConnection.getAdmin
            if (!admin.tableExists(TableName.valueOf(dataSourceInfo.dbName + ":" + dataSourceInfo.tableName))) {
                admin.close()
                throw new TableNotFoundException("表 " + dataSourceInfo.tableName + " 不存在!")
            }
            //从HBase中取出所有所选的列
            val rowData = columns.split(",").flatMap(fields => {
                val familyAndColumns = fields.split(":")
                familyAndColumns.drop(1).map(column => (familyAndColumns(0), column))
            })
            //创建一个扫描对象
            val scan = new Scan()
            //拿到表对象
            val table = hbaseConnection.getTable(TableName.valueOf(dataSourceInfo.dbName + ":" + dataSourceInfo.tableName))
            //将列添加到扫描对象中
            if (rowData != null) rowData.map(col => scan.addColumn(col._1.getBytes, col._2.getBytes))
            //全表扫描的结果集
            val scanner = table.getScanner(scan)
            //结果变量
            var result: Result = null
            //遍历结果集
            while ( {
                result = scanner.next()
                result != null
            }) {
                //取出每个结果的列值
                val values = result.listCells().map(cell => {
                    new String(CellUtil.cloneValue(cell))
                }).toArray
                //将结果重新加入集合
                resultSet += values
            }
            //释放资源
            scanner.close()
            admin.close()
            hbaseConnection.close()
        } catch {
            case e: Exception => e.printStackTrace()
        }
        //返回结果集
        resultSet
    }

    /**
      * 拿到HBase全表数据
      *
      * @param dataSourceInfo 数据源信息
      * @param principal      Kerberos个人凭证
      * @param keytab         Kerberos keytab 文件地址
      * @return 所有的列族和列名
      */
    def getAllColumns(dataSourceInfo: DataSourceInfo, principal: String, keytab: String): ArrayBuffer[Array[String]] = {
        //结果数据集数组
        val resultSet: ArrayBuffer[Array[String]] = ArrayBuffer()
        //初始化HBase连接
        initHBaseConnection(dataSourceInfo, principal, keytab)
        val admin = hbaseConnection.getAdmin
        if (!admin.tableExists(TableName.valueOf(dataSourceInfo.dbName + ":" + dataSourceInfo.tableName))) {
            admin.close()
            throw new TableNotFoundException("表 " + dataSourceInfo.tableName + " 不存在!")
        }
        //得到多行种字段最多的作为待读取的列名
        var maxCells = 0
        //创建一个扫描对象
        val scan = new Scan()
        //定义一个结果扫描对象
        var resultScanner: ResultScanner = null
        //获取表对象
        var table: Table = null
        try {
            table = hbaseConnection.getTable(TableName.valueOf(dataSourceInfo.dbName + ":" + dataSourceInfo.tableName))
            resultScanner = table.getScanner(scan)
            for (result: Result <- resultScanner) {
                //对每一行数据进行处理,拿到所有的值
                val values = result.listCells().map(cell => Bytes.toString(CellUtil.cloneValue(cell))).toArray
                //将结果放到数组中
                resultSet += values
                if (maxCells < result.listCells().size()) {
                    //将列族名和列名存入集合
                    columnNames = result.listCells().map(cell => (Bytes.toString(CellUtil.cloneFamily(cell)), Bytes.toString(CellUtil.cloneQualifier(cell)))).toArray
                    //更新行的最大字段数
                    maxCells = result.listCells().size()
                }
            }
            admin.close()
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            hbaseConnection.close()
        }
        resultSet
    }

    /**
      * 删除rowKey的某簇的某列
      *
      * @param rowKey 行键
      * @param family 列簇
      * @param column 列名
      */
    def deleteByColumn(rowKey: String, family: String, column: String, dataSourceInfo: DataSourceInfo, principal: String, keytab: String): Unit = {
        try {
            //初始化HBase连接
            initHBaseConnection(dataSourceInfo, principal, keytab)
            val delete = new Delete(rowKey.getBytes())
            delete.addColumn(family.getBytes(), column.getBytes())
            val table = hbaseConnection.getTable(TableName.valueOf(dataSourceInfo.tableName))
            table.delete(delete)
        } catch {
            case e: Exception => println(">>>删除操作失败：" + e)
        }

    }

}
