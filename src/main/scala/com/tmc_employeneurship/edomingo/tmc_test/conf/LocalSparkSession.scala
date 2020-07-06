package com.tmc_employeneurship.edomingo.tmc_test.conf

import org.apache.spark.sql.SparkSession

trait LocalSparkSession {
  lazy val spark_hive: SparkSession = {
    SparkSession
      .builder()
      .master("spark://localhost:7077")
      .appName("spark hive test")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .config("fs.defaultFS", "hdfs://namenode:8020")
      .enableHiveSupport()
      .getOrCreate()
  }

  lazy val spark_s3: SparkSession = {
    SparkSession
      .builder()
      .master("spark://localhost:7077")
      .appName("spark s3 test")
      .config("spark.hadoop.fs.s3a.access.key", "secret")
      .config("spark.hadoop.fs.s3a.secret.key", "secret")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false")
      .getOrCreate()
  }
}
