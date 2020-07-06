package com.tmc_employeneurship.edomingo.tmc_test.api

import org.apache.spark.sql.{DataFrame, SaveMode}

object APIFactory {

  object APIType extends Enumeration {
    val HIVE = Value("hive")
    val S3 = Value("s3")
  }

  private class HiveAPI extends API {

    def getTable(tableName: String) = spark_hive.sqlContext.read.table(tableName)

    def writeTable(df: DataFrame, tableName: String, saveMode: SaveMode) = df.write.mode(saveMode).format("hive").saveAsTable(tableName)
  }

  private class S3API  extends API {

    def getTable(tableName: String) = spark_s3.sqlContext.read.table(tableName)

    def writeTable(df: DataFrame, tableName: String, saveMode: SaveMode) = df.write.mode(saveMode).format("csv")
      .option("header","true").save(String.format("s3a://test/%s", tableName))
  }

  def apply(apiType: APIType.Value): API = {
    apiType match {
      case APIType.HIVE => new HiveAPI
      case _ => new S3API
    }
  }
}
