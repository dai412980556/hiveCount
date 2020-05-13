package Utils

import java.io.{FileSystem => _, _}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import scala.collection.mutable.ListBuffer

/**
  * hdfs 文件操作工具
  */
object HDFSUtil {

    def isDir(hdfs: FileSystem, name: String): Boolean = {
        hdfs.isDirectory(new Path(name))
    }

    def isDir(hdfs: FileSystem, name: Path): Boolean = {
        hdfs.isDirectory(name)
    }

    def isFile(hdfs: FileSystem, name: String): Boolean = {
        hdfs.isFile(new Path(name))
    }

    def isFile(hdfs: FileSystem, name: Path): Boolean = {
        hdfs.isFile(name)
    }

    def createFile(hdfs: FileSystem, name: String): Boolean = {
        hdfs.createNewFile(new Path(name))
    }

    def createFile(hdfs: FileSystem, name: Path): Boolean = {
        hdfs.createNewFile(name)
    }

    def createFolder(hdfs: FileSystem, name: String): Boolean = {
        hdfs.mkdirs(new Path(name))
    }

    def createFolder(hdfs: FileSystem, name: Path): Boolean = {
        hdfs.mkdirs(name)
    }

    def exists(hdfs: FileSystem, name: String): Boolean = {
        hdfs.exists(new Path(name))
    }

    def exists(hdfs: FileSystem, name: Path): Boolean = {
        hdfs.exists(name)
    }

    def transport(inputStream: InputStream, outputStream: OutputStream): Unit = {
        val buffer = new Array[Byte](64 * 1000)
        var len = inputStream.read(buffer)
        while (len != -1) {
            outputStream.write(buffer, 0, len - 1)
            len = inputStream.read(buffer)
        }
        outputStream.flush()
        inputStream.close()
        outputStream.close()
    }

    class MyPathFilter extends PathFilter {
        override def accept(path: Path): Boolean = true
    }

    /**
      * 创建目标文件并在必要时提供父文件夹
      */
    def createLocalFile(fullName: String): File = {
        val target: File = new File(fullName)
        if (!target.exists) {
            val index = fullName.lastIndexOf(File.separator)
            val parentFullName = fullName.substring(0, index)
            val parent: File = new File(parentFullName)

            if (!parent.exists)
                parent.mkdirs
            else if (!parent.isDirectory)
                parent.mkdir

            target.createNewFile
        }
        target
    }

    /**
      * 删除在hdfs上的文件
      *
      * @return true: 成功, false: 失败
      */
    def deleteFile(hdfs: FileSystem, path: String): Boolean = {
        if (isDir(hdfs, path))
            hdfs.delete(new Path(path), true) //true: delete files recursively
        else
            hdfs.delete(new Path(path), false)
    }

    /**
      * 获取hdfs目录中所有文件子目录的全名,不包括dir子目录
      *
      * @param fullName hdfs目录的全名
      */
    def listChildren(hdfs: FileSystem, fullName: String, holder: ListBuffer[String]): ListBuffer[String] = {
        val filesStatus = hdfs.listStatus(new Path(fullName), new MyPathFilter)
        for (status <- filesStatus) {
            val filePath: Path = status.getPath
            if (isFile(hdfs, filePath))
                holder += filePath.toString
            else
                listChildren(hdfs, filePath.toString, holder)
        }
        holder
    }

    def copyFile(hdfs: FileSystem, source: String, target: String): Unit = {

        val sourcePath = new Path(source)
        val targetPath = new Path(target)

        if (!exists(hdfs, targetPath))
            createFile(hdfs, targetPath)

        val inputStream: FSDataInputStream = hdfs.open(sourcePath)
        val outputStream: FSDataOutputStream = hdfs.create(targetPath)
        transport(inputStream, outputStream)
    }

    def copyFolder(hdfs: FileSystem, sourceFolder: String, targetFolder: String): Unit = {
        val holder: ListBuffer[String] = new ListBuffer[String]
        val children: List[String] = listChildren(hdfs, sourceFolder, holder).toList
        for (child <- children)
            copyFile(hdfs, child, child.replaceFirst(sourceFolder, targetFolder))
    }

    def copyFileFromLocal(hdfs: FileSystem, localSource: String, hdfsTarget: String): Unit = {
        val targetPath = new Path(hdfsTarget)
        if (!exists(hdfs, targetPath))
            createFile(hdfs, targetPath)

        val inputStream: FileInputStream = new FileInputStream(localSource)
        val outputStream: FSDataOutputStream = hdfs.create(targetPath)
        transport(inputStream, outputStream)
    }

    def copyFileToLocal(hdfs: FileSystem, hdfsSource: String, localTarget: String): Unit = {
        val localFile: File = createLocalFile(localTarget)

        val inputStream: FSDataInputStream = hdfs.open(new Path(hdfsSource))
        val outputStream: FileOutputStream = new FileOutputStream(localFile)
        transport(inputStream, outputStream)
    }

    def copyFolderFromLocal(hdfs: FileSystem, localSource: String, hdfsTarget: String): Unit = {
        val localFolder: File = new File(localSource)
        val allChildren: Array[File] = localFolder.listFiles
        for (child <- allChildren) {
            val fullName = child.getAbsolutePath
            val nameExcludeSource: String = fullName.substring(localSource.length)
            val targetFileFullName: String = hdfsTarget + Path.SEPARATOR + nameExcludeSource
            if (child.isFile)
                copyFileFromLocal(hdfs, fullName, targetFileFullName)
            else
                copyFolderFromLocal(hdfs, fullName, targetFileFullName)
        }
    }

    def copyFolderToLocal(hdfs: FileSystem, hdfsSource: String, localTarget: String): Unit = {
        val holder: ListBuffer[String] = new ListBuffer[String]
        val children: List[String] = listChildren(hdfs, hdfsSource, holder).toList
        val hdfsSourceFullName = hdfs.getFileStatus(new Path(hdfsSource)).getPath.toString
        val index = hdfsSourceFullName.length
        for (child <- children) {
            val nameExcludeSource: String = child.substring(index + 1)
            val targetFileFullName: String = localTarget + File.separator + nameExcludeSource
            copyFileToLocal(hdfs, child, targetFileFullName)
        }
    }


    /**
      * 将文件保存到hdfs
      *
      * @param conf     Hadoop配置
      * @param fileName 文件名
      * @param data     要写入的数据
      */
    def saveAsFile(conf: Configuration, fileName: String, data: String): Unit = {
        try {
            //创建一个hdfs文件实例
            val hdfs = FileSystem.get(
                URI.create(conf.get("fs.default.name")), conf)
            //创建一个目录
            if (hdfs.exists(new Path(fileName))) {
                hdfs.delete(new Path(fileName), false)
            }
            //创建一个输出流
            val out = hdfs.create(new Path(fileName), true)
            //将文件写入字节数组
            out.write(data.getBytes())
            //刷新流
            out.flush()
            //关闭流
            out.close()
        } catch {
            case e: IOException => e.printStackTrace()
            case e: Exception => e.printStackTrace()
        }

    }

    /**
      * 从hdfs中读取文件
      *
      * @param conf     hadoopConfig
      * @param fileName 文件名
      * @return 文件内容
      */
    def readFromHDFS(conf: Configuration, fileName: String): String = {
        try {
            val hdfs = FileSystem.get(
                URI.create(conf.get("fs.default.name")), conf)
            //读取HDFS系统中的一个文件
            val input = hdfs.open(new Path(fileName))
            val b = new Array[Byte](1024)
            input.read(b, 0, b.length)
            val str = String.valueOf(b)
            print(str)
            input.close()
            str

        } catch {
            case e: IOException => e.getStackTrace.mkString(",")
            case e: Exception => e.getStackTrace.mkString(",")
        }

    }

    /**
      * 获取hdfs某个目录下所有一级文件和目录(非路径)
      *
      * @param hdfs     hdfs文件对象
      * @param fullName 全路径
      */
    def getFilesAndDirs(hdfs: FileSystem, fullName: String): Array[String] = {
        var listFiles = Array[String]()
        if (isDir(hdfs, fullName)) {
            val fileStatus = hdfs.listStatus(new Path(fullName))

            listFiles = FileUtil.stat2Paths(fileStatus).map(path => {
                val strPath = path.toString
                strPath.split("/").takeRight(1)(0)
            })
        }
        listFiles
    }


    /**
      * 获取hdfs某个目录下所有一级文件和目录(路径)
      *
      * @param hdfs     hdfs文件对象
      * @param fullName 路径名
      * @return 下层所有条目列表
      */
    def getNextList(hdfs: FileSystem, fullName: String): Array[String] = {
        val fileStatus = hdfs.listStatus(new Path(fullName))
        val listFiles = FileUtil.stat2Paths(fileStatus).map(_.toString)
        listFiles
    }

    /**
      * 获取hdfs文件大小
      *
      * @param hdfs hdfs文件系统对象
      * @param path 绝对路径
      */
    def getFileSize(hdfs: FileSystem, path: String): String = {
        hdfs.getContentSummary(new Path(path)).getLength.toString
    }

    /**
      * 获取文件最后修改时间
      *
      * @param hdfs hadoop文件系统
      * @param path 路径
      * @return 所有文件的最后修改时间
      */
    def getLastUpdateTime(hdfs: FileSystem, path: String): Array[Long] = {
        hdfs.listStatus(new Path(path)).map(file => file.getModificationTime)
    }

}
