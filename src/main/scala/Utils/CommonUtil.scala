package Utils

import java.text.SimpleDateFormat
import java.util.Date

import com.google.gson.{Gson, JsonSyntaxException}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

object CommonUtil {

    /**
      * 字符型时间格式
      */
    val untilSecondFormat = "yyyy-MM-dd HH:mm:ss"

    /**
      * 初始化gson解析工具
      */
    val gson = new Gson()

    /**
      * 判断字符串是否为空
      *
      * @param str 传入的字符串
      * @return 是否为空
      */
    def isEmpty(str: String): Boolean = {

        if (null == str || str.length == 0) true else false

    }

    /**
      * 用空字符串代替null值
      *
      * @param str 传入的字符串
      * @return 返回的字符值
      */
    def replaceNull(str: String): String = {
        if (null == str) "" else str
    }

    /**
      * 判断字符串是否为Int类型
      *
      * @param str 要判断的字符串
      * @return 是否是Int
      */
    def isIntByRegex(str: String): Boolean = {
        val pattern = """^(\d+)$""".r
        str match {
            case pattern(_*) => true
            case _ => false
        }
    }

    /**
      * 判断字符串是否为Double类型
      *
      * @param str 字符串
      * @return 是否为Double
      */
    def isDoubleByRegex(str: String): Boolean = {
        val pattern = """^[-\+]?\d*[.]\d+$""".r
        str match {
            case pattern(_*) => true
            case _ => false
        }
    }


    /**
      * 修改DataFrame的scheme属性
      *
      * @param df       输入的数据集
      * @param cn       列名
      * @param nullable 是否可以为空
      * @return 修改后的数据集
      */
    def setNullableStateOfColumn(df: DataFrame, cn: String, nullable: Boolean): DataFrame = {

        // 拿到scheme
        val schema = df.schema
        // 修改 [[StructField] with name `cn`
        val newSchema = StructType(schema.map {
            case StructField(c, t, _, m) if c.equals(cn) => StructField(c, t, nullable = nullable, m)
            case y: StructField => y
        })
        // 应用新的schema
        df.sqlContext.createDataFrame(df.rdd, newSchema)
    }

    /**
      * 解析字符串是否是Json对象
      *
      * @param str 要解析的字符串
      * @return 是否是Json对象
      */
    def isJsonObject(str: String): Boolean = {

        try {
            gson.fromJson(str, classOf[Any])
            true
        } catch {
            case e: JsonSyntaxException => false
        }

    }

    /**
      * 将时间戳格式转换成固定格式的字符串格式
      *
      * @param timeStamp 要转换的时间戳
      * @return 格式化的时间
      */
    def tranTimeToString(timeStamp: Long): String = {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val time = dateFormat.format(new Date(timeStamp))
        time
    }


}
