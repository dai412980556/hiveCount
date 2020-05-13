package Utils

import java.io.{FileInputStream, FileNotFoundException, IOException, InputStream}
import java.util.Properties

import org.slf4j.LoggerFactory

/**
  * 加载配置文件工具类
  */
object PropertyUtil {

    private val logger = LoggerFactory.getLogger(PropertyUtil.getClass)
    private var props: Properties = _
    private var in: InputStream = _

    loadProps()

    def init(path: String): Unit = {

        props = new Properties()

        try {
            logger.error("进入读文件阶段。。。。。。。。。。")
            in = new FileInputStream(path)
            props.load(in)
        } catch {
            case e: FileNotFoundException =>
                logger.error(path)
                logger.error("config.properties文件未找到")
            case e: IOException =>
                logger.error("出现IOException")
        } finally {
            try {
                if (null != in) in.close()
            }
            catch {
                case e: IOException =>
                    logger.error("jdbc.properties文件流关闭出现异常")
            }
            logger.info("加载properties文件内容完成...........")
            logger.info("properties文件内容：" + props)
        }

    }

    def loadProps(): Unit = {
        logger.info("开始加载properties文件内容.......")
        props = new Properties()

        try {

            //<!--通过类加载器进行获取properties文件流-->
            in = PropertyUtil.getClass.getClassLoader.getResourceAsStream("config.properties")
            props.load(in)
        } catch {
            case e: FileNotFoundException =>
                logger.error("config.properties文件未找到")
            case e: IOException =>
                logger.error("出现IOException")
        } finally {
            try {
                if (null != in) in.close()
            }
            catch {
                case e: IOException =>
                    logger.error("jdbc.properties文件流关闭出现异常")
            }
            logger.info("加载properties文件内容完成...........")
            logger.info("properties文件内容：" + props)
        }
    }

    def getProperty(key: String): String = {
        if (null == props) loadProps()
        props.getProperty(key)
    }

    def getProperty(key: String, defaultValue: String): String = {
        if (null == props) loadProps()
        props.getProperty(key, defaultValue)
    }

    def getJsonConfig: String = {

        props.toString

    }


}
