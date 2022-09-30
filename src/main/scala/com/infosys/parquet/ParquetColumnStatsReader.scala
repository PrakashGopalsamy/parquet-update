package com.infosys.parquet

import com.infosys.parquet.exception.RequiredPropException
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{from_json, col}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.util
import collection.JavaConverters._

class ParquetColumnStatsReader (spark: SparkSession, analysisColumns: String, basePath: String){

  validateRequiredProps()

  def frameMetadataDF(): DataFrame = {
    val filePaths = new ParquetFileNamesReader().getParquetFiles(basePath)
    val metadataJson = filePaths.map(filePath => new ColumnStatsExtractor(analysisColumns).extractColumnStats(filePath))
    val baseRdd = spark.sparkContext.parallelize(metadataJson).map(Row(_))
    val baseDF = spark.createDataFrame(baseRdd, StructType(Seq(StructField("json",StringType,true))))
    baseDF.withColumn("metadata",from_json(col("json"),frameSchema())).select(col("metadata.*"))
  }

  private def validateRequiredProps(): Unit = {
    if (analysisColumns == null || analysisColumns.isEmpty){
      throw new RequiredPropException(s"The required Property analysisColumns not given")
    }
    if (basePath == null || basePath.isEmpty){
      throw new RequiredPropException(s"The required Property basePath not given")
    }
  }

  private def frameSchema(): StructType ={
    val fields = new util.ArrayList[StructField]()
    fields.add(new StructField("filename",StringType,true))
    analysisColumns.split(",").map(_.toLowerCase()).foreach(column => {
      fields.add(new StructField(s"${column}_min",StringType,true))
      fields.add(new StructField(s"${column}_max",StringType,true))
    })
    StructType(fields.asScala)
  }

}
