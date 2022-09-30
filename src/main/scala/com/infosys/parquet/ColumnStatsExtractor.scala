package com.infosys.parquet

import com.fasterxml.jackson.databind.ObjectMapper
import com.infosys.parquet.exception.{NonPrimitiveException, UnSupportedDataTypeException}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.{ColumnChunkMetaData, ParquetMetadata}
import org.apache.parquet.hadoop.util.HadoopInputFile

import collection.mutable.{HashMap => MutableHashMap}
import collection.JavaConverters._

class ColumnStatsExtractor(val analysisColumn: String) {

  private val analysisCols  = analysisColumn.split(",").map(_.toLowerCase()).toList

  private val hadoopConf = new Configuration()

  private val parquetReadOptions = ParquetReadOptions.builder().build()

  def extractColumnStats(parquetFilePath: String): String = {
    extractColumnStats(createHdfsPath(parquetFilePath))
  }

  def extractColumnStats(parquetFilePath: Path): String = {
    val parquetMetadata = readFooter(parquetFilePath)
    val columnChunkMetaData = extractColumnChunkMetadata(parquetMetadata)
    val metadata = if (columnChunkMetaData.nonEmpty){
      validateDataType(columnChunkMetaData)
      val colStats = new MutableHashMap[String,(String, String)]()
      columnChunkMetaData.foreach(chunkMetaData => {
        extractColStats(chunkMetaData,colStats)
      })
      frameMetaDataString(parquetFilePath.toUri.toString,colStats)
    } else{
      ""
    }
    metadata
  }

  private def createHdfsPath(filePath: String): Path = {
    new Path(filePath)
  }

  private def readFooter(parquetFilePath: Path): ParquetMetadata = {
    ParquetFileReader.open(HadoopInputFile.fromPath(parquetFilePath,hadoopConf),parquetReadOptions).getFooter
  }

  private def extractColumnChunkMetadata(parquetMetadata: ParquetMetadata): List[ColumnChunkMetaData] = {
    parquetMetadata.getBlocks.asScala.flatMap(_.getColumns.asScala).filter(chnkMetadata => analysisCols.contains(chnkMetadata.getPath.toDotString.toLowerCase)).toList
  }

  private def validateDataType(chunkMetaDatas: List[ColumnChunkMetaData]): Unit = {
    chunkMetaDatas.foreach(chunkMetaData => {
      val columnName = chunkMetaData.getPath.toDotString
      val primitiveType = chunkMetaData.getPrimitiveType
      if (!primitiveType.isPrimitive){
        throw new NonPrimitiveException(s"DataType of the column $columnName is Non-Primitive. Column Stats collecting is enabled only for primitive datatypes")
      }
      val dataType = primitiveType.getPrimitiveTypeName.javaType
      if (!(dataType == classOf[org.apache.parquet.io.api.Binary])){
        throw new UnSupportedDataTypeException(s"DataType - $dataType of the column $columnName is not supported. Only String DataType is supported")
      }
    })
  }

  private def extractColStats(chunkMetadata: ColumnChunkMetaData, colStats: MutableHashMap[String,(String, String)]): Unit ={
    val columnName = chunkMetadata.getPath.toDotString
    val minValue = chunkMetadata.getStatistics.minAsString
    val maxValue = chunkMetadata.getStatistics.maxAsString
    val prevStats = colStats.get(columnName)
    if (prevStats.isEmpty)
    {
      colStats.put(columnName,(minValue,maxValue))
    } else{
      val finalMinVal = if (minValue <= prevStats.get._1) minValue else prevStats.get._1
      val finalMaxVal = if (maxValue >= prevStats.get._2) maxValue else prevStats.get._2
      colStats.put(columnName,(finalMinVal,finalMaxVal))
    }
  }

  private def frameMetaDataString(fileName: String, colStats: MutableHashMap[String,(String, String)]): String = {
    val objectMapper = new ObjectMapper()
    var metaData = objectMapper.createObjectNode()
    metaData.put("filename",fileName)
    analysisCols.foreach(column =>
    {
      val min = if (!colStats.contains(column)) null else colStats(column)._1
      val max = if (!colStats.contains(column)) null else colStats(column)._2
      metaData.put(s"${column}_min",min)
      metaData.put(s"${column}_max",max)
    }
    )
    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(metaData)
  }
}
