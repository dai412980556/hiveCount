package bean

import scala.beans.BeanProperty

/**
  * 数据仓库信息
  */
class DataSourceInfo {
    /**
      * 节点ID
      */
    @BeanProperty
    var nodeId: Int = _
    /**
      * 数据集ID
      */
    @BeanProperty
    var datasetId: String = _
    /**
      * 节点IP
      */
    @BeanProperty
    var ip: String = _
    /**
      * 端口
      */
    @BeanProperty
    var port: Int = _
    /**
      * 数据库名称
      */
    @BeanProperty
    var dbName: String = _
    /**
      * 表名
      */
    @BeanProperty
    var tableName: String = _
    /**
      * 数据库类型
      */
    @BeanProperty
    var dbType: String = _

    /**
      * 重写toString方法
      *
      * @return 数据源信息
      */
    override def toString = s"DataSourceInfo($nodeId, $datasetId, $ip, $port, $dbName, $tableName, $dbType)"

}
