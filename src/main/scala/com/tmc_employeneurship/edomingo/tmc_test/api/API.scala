package com.tmc_employeneurship.edomingo.tmc_test.api

import com.tmc_employeneurship.edomingo.tmc_test.conf.LocalSparkSession
import org.apache.spark.sql.{DataFrame, SaveMode}

trait API extends LocalSparkSession {

  def getTable(tableName: String): DataFrame

  def writeTable(df: DataFrame, tableName: String, saveMode: SaveMode)
}
