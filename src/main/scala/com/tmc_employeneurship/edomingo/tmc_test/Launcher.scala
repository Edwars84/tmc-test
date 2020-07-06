package com.tmc_employeneurship.edomingo.tmc_test

import com.tmc_employeneurship.edomingo.tmc_test.api.APIFactory
import org.apache.spark.sql.SaveMode

object Launcher extends App {

  val hiveApi = APIFactory(APIFactory.APIType.HIVE)
  val s3Api = APIFactory(APIFactory.APIType.S3)

  val dfFromHive = hiveApi.getTable("csv_test")

  // Generate output columns in the result dataset with these changes:
  val p1 = DataFrameGenerator.getMostBirthdaysOn1Day(dfFromHive)

  val p2 = DataFrameGenerator.getEmailProvidersMore10kPosts(dfFromHive)

  val p3 = DataFrameGenerator.getNumPostByEmailProvider(dfFromHive)

  val p4 = DataFrameGenerator.getYearsMaxSignUps(dfFromHive)

  val p5 = DataFrameGenerator.getIpFrequencyByOctet(dfFromHive, 1)

  val p6 = DataFrameGenerator.getIpFrequencyByOctet(dfFromHive, 3)

  val p7 = DataFrameGenerator.getReferralsByMembers(dfFromHive)

  //Write results in hive or aws s3 (or both of them)
  hiveApi.writeTable(p1, "p1", SaveMode.Overwrite)
  hiveApi.writeTable(p2, "p2", SaveMode.Overwrite)
  hiveApi.writeTable(p3, "p3", SaveMode.Overwrite)
  hiveApi.writeTable(p4, "p4", SaveMode.Overwrite)
  hiveApi.writeTable(p5, "p5", SaveMode.Overwrite)
  hiveApi.writeTable(p6, "p6", SaveMode.Overwrite)
  hiveApi.writeTable(p7, "p7", SaveMode.Overwrite)

//  s3Api.writeTable(p1, "p1", SaveMode.Overwrite)
//  s3Api.writeTable(p2, "p2", SaveMode.Overwrite)
//  s3Api.writeTable(p3, "p3", SaveMode.Overwrite)
//  s3Api.writeTable(p4, "p4", SaveMode.Overwrite)
//  s3Api.writeTable(p5, "p5", SaveMode.Overwrite)
//  s3Api.writeTable(p6, "p6", SaveMode.Overwrite)
//  s3Api.writeTable(p7, "p7", SaveMode.Overwrite)
}
