package com.infosys.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import java.util
import scala.collection.mutable
import collection.JavaConverters._

class ParquetFileNamesReader {

  private val hdfsFS = FileSystem.get(new Configuration())

  private val fileQueue = mutable.Queue[FileStatus]()

  def getParquetFiles(basePath: String): List[String] = {
    val hdfsFiles = hdfsFS.listStatus(new Path(basePath))
    hdfsFiles.foreach(file => fileQueue.enqueue(file))
    getFileNames()
  }

  private def getFileNames(): List[String] = {
    val fileNames = new util.ArrayList[String]()
    while (!fileQueue.isEmpty){
      val currFile = fileQueue.dequeue()
      if (currFile.isFile){
        val filePath = currFile.getPath.toUri.toString
        if (filePath.endsWith(".parquet")){
          fileNames.add(filePath)
        }
      } else{
        hdfsFS.listStatus(currFile.getPath).foreach(file => fileQueue.enqueue(file))
      }
    }
    fileNames.asScala.toList
  }

}
