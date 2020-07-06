package com.tmc_employeneurship.edomingo.tmc_test

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfter, FunSuite}

class DataframeGeneratorTest extends FunSuite with BeforeAndAfter {

  var spark: SparkSession = _

  val schema = List(
    StructField("member_id", IntegerType, true),
    StructField("name", StringType, true),
    StructField("email", StringType, true),
    StructField("joined", LongType, true),
    StructField("ip_address", StringType, true),
    StructField("posts", IntegerType, true),
    StructField("bday_day", IntegerType, true),
    StructField("bday_month", IntegerType, true),
    StructField("bday_year", IntegerType, true),
    StructField("members_profile_views", IntegerType, true),
    StructField("referred_by", IntegerType, true)
  )

  before {
    spark = SparkSession.builder
      .master("local[1]")
      .appName("testing")
      .getOrCreate()
  }

  after {
    spark.sparkContext.stop()
  }

  test("Generate a query to obtain a most birthdays on 1 day") {
    val data = Seq(
      Row(0, "a", "a@a.com", 0.toLong, "0.0.0.0", 0, 1, 1, 1900, 0, 0),
      Row(1, "b", "b@b.com", 0.toLong, "0.0.0.0", 0, 1, 1, 1901, 0, 0),
      Row(2, "c", "c@c.com", 0.toLong, "0.0.0.0", 0, 2, 1, 1900, 0, 0),
      Row(3, "d", "d@d.com", 0.toLong, "0.0.0.0", 0, 0, 0, 0, 0, 0)
    )

    val mockDF = spark.createDataFrame(spark.sparkContext.parallelize(data), StructType(schema))

    val result = DataFrameGenerator.getMostBirthdaysOn1Day(mockDF)

    assert(result.count() == 1)
    assert(result.columns.length == 3)
    val resultAsList = result.collectAsList()
    assert((resultAsList.get(0).getAs[Int]("bday_day") == 1)
      && (resultAsList.get(0).getAs[Int]("bday_month") == 1)
      && (resultAsList.get(0).getAs[Long]("num_bdays") == 2))
  }

  test("Email providers with more than 10k posts") {
    val data = Seq(
      Row(0, "a", "a@1.com", 0.toLong, "0.0.0.0", 9000, 1, 1, 1900, 0, 0),
      Row(1, "b", "b@1.com", 0.toLong, "0.0.0.0", 1001, 1, 1, 1901, 0, 0),
      Row(2, "c", "c@2.com", 0.toLong, "0.0.0.0", 10001, 2, 1, 1900, 0, 0),
      Row(3, "d", "d@3.com", 0.toLong, "0.0.0.0", 10000, 0, 0, 0, 0, 0),
      Row(4, "e", "d@4.com", 0.toLong, "0.0.0.0", 9999, 0, 0, 0, 0, 0)
    )

    val mockDF = spark.createDataFrame(spark.sparkContext.parallelize(data), StructType(schema))

    val result = DataFrameGenerator.getEmailProvidersMore10kPosts(mockDF)

    assert(result.count() == 2)
    assert(result.columns.length == 1)
    val resultAsList = result.collectAsList()
    assert(resultAsList.contains(Row("2.com")))
    assert(resultAsList.contains(Row("1.com")))
  }

  test("Post by email providers") {
    val data = Seq(
      Row(0, "a", "a@1.com", 0.toLong, "0.0.0.0", 9000, 1, 1, 1900, 0, 0),
      Row(1, "b", "b@1.com", 0.toLong, "0.0.0.0", 1001, 1, 1, 1901, 0, 0),
      Row(2, "c", "c@2.com", 0.toLong, "0.0.0.0", 10001, 2, 1, 1900, 0, 0),
      Row(3, "d", "d@3.com", 0.toLong, "0.0.0.0", 10000, 0, 0, 0, 0, 0),
      Row(4, "e", "d@4.com", 0.toLong, "0.0.0.0", 9999, 0, 0, 0, 0, 0)
    )

    val mockDF = spark.createDataFrame(spark.sparkContext.parallelize(data), StructType(schema))

    val result = DataFrameGenerator.getNumPostByEmailProvider(mockDF)

    assert(result.count() == 4)
    assert(result.columns.length == 2)
    val resultAsList = result.collectAsList()
    assert(resultAsList.contains(Row("1.com", 10001)))
    assert(resultAsList.contains(Row("2.com", 10001)))
    assert(resultAsList.contains(Row("3.com", 10000)))
    assert(resultAsList.contains(Row("4.com", 9999)))
  }

  test("Year/s with max sign ups") {
    val data = Seq(
      Row(0, "a", "a@1.com", 1593969000.toLong, "0.0.0.0", 9000, 1, 1, 1900, 0, 0),
      Row(1, "b", "b@1.com", 1593968000.toLong, "0.0.0.0", 1001, 1, 1, 1901, 0, 0),
      Row(2, "c", "c@2.com", 1593967000.toLong, "0.0.0.0", 10001, 2, 1, 1900, 0, 0),
      Row(3, "d", "d@3.com", 1569888000.toLong, "0.0.0.0", 10000, 0, 0, 0, 0, 0),
      Row(4, "e", "e@4.com", 1569887000.toLong, "0.0.0.0", 9999, 0, 0, 0, 0, 0),
      Row(5, "f", "f@4.com", 1569886000.toLong, "0.0.0.0", 9999, 0, 0, 0, 0, 0),
      Row(4, "g", "g@4.com", 1369887000.toLong, "0.0.0.0", 9999, 0, 0, 0, 0, 0),
      Row(5, "h", "h@4.com", 1169886000.toLong, "0.0.0.0", 9999, 0, 0, 0, 0, 0),
      Row(4, "i", "i@4.com", 1069887000.toLong, "0.0.0.0", 9999, 0, 0, 0, 0, 0),
      Row(5, "j", "j@4.com", 869886000.toLong, "0.0.0.0", 9999, 0, 0, 0, 0, 0)
    )

    val mockDF = spark.createDataFrame(spark.sparkContext.parallelize(data), StructType(schema))

    val result = DataFrameGenerator.getYearsMaxSignUps(mockDF)

    assert(result.count() == 2)
    assert(result.columns.length == 2)
    val resultAsList = result.collectAsList()
    assert(resultAsList.contains(Row(2020, 3)))
    assert(resultAsList.contains(Row(2019, 3)))

  }

  test("Class C IP address frequency by 1st octet") {
    val data = Seq(
      Row(0, "a", "a@1.com", 1593969000.toLong, "0.0.0.0", 9000, 1, 1, 1900, 0, 0),
      Row(1, "b", "b@1.com", 1593968000.toLong, "1.0.0.0", 1001, 1, 1, 1901, 0, 0),
      Row(2, "c", "c@2.com", 1593967000.toLong, "1.0.0.0", 10001, 2, 1, 1900, 0, 0),
      Row(3, "d", "d@3.com", 1569888000.toLong, "2.0.0.0", 10000, 0, 0, 0, 0, 0),
      Row(4, "e", "e@4.com", 1569887000.toLong, "2.0.0.0", 9999, 0, 0, 0, 0, 0),
      Row(5, "f", "f@4.com", 1569886000.toLong, "2.0.0.0", 9999, 0, 0, 0, 0, 0),
      Row(4, "g", "g@4.com", 1369887000.toLong, "255.0.0.0", 9999, 0, 0, 0, 0, 0),
      Row(5, "h", "h@4.com", 1169886000.toLong, "255.0.0.0", 9999, 0, 0, 0, 0, 0),
      Row(4, "i", "i@4.com", 1069887000.toLong, "255.0.0.0", 9999, 0, 0, 0, 0, 0),
      Row(5, "j", "j@4.com", 869886000.toLong, "255.0.0.0", 9999, 0, 0, 0, 0, 0)
    )

    val mockDF = spark.createDataFrame(spark.sparkContext.parallelize(data), StructType(schema))

    val result = DataFrameGenerator.getIpFrequencyByOctet(mockDF,1)

    assert(result.count() == 4)
    assert(result.columns.length == 2)
    val resultAsList = result.collectAsList()
    assert(resultAsList.contains(Row("0", 1)))
    assert(resultAsList.contains(Row("1", 2)))
    assert(resultAsList.contains(Row("2", 3)))
    assert(resultAsList.contains(Row("255", 4)))
  }

  test("Frequency of IP address based on first 3 octets") {
    val data = Seq(
      Row(0, "a", "a@1.com", 1593969000.toLong, "0.0.0.0", 9000, 1, 1, 1900, 0, 0),
      Row(1, "b", "b@1.com", 1593968000.toLong, "1.0.0.0", 1001, 1, 1, 1901, 0, 0),
      Row(2, "c", "c@2.com", 1593967000.toLong, "1.0.0.2", 10001, 2, 1, 1900, 0, 0),
      Row(3, "d", "d@3.com", 1569888000.toLong, "2.0.0.0", 10000, 0, 0, 0, 0, 0),
      Row(4, "e", "e@4.com", 1569887000.toLong, "2.0.0.0", 9999, 0, 0, 0, 0, 0),
      Row(5, "f", "f@4.com", 1569886000.toLong, "2.0.0.1", 9999, 0, 0, 0, 0, 0),
      Row(4, "g", "g@4.com", 1369887000.toLong, "255.0.0.0", 9999, 0, 0, 0, 0, 0),
      Row(5, "h", "h@4.com", 1169886000.toLong, "255.0.0.0", 9999, 0, 0, 0, 0, 0),
      Row(4, "i", "i@4.com", 1069887000.toLong, "255.1.0.0", 9999, 0, 0, 0, 0, 0),
      Row(5, "j", "j@4.com", 869886000.toLong, "255.0.1.0", 9999, 0, 0, 0, 0, 0)
    )

    val mockDF = spark.createDataFrame(spark.sparkContext.parallelize(data), StructType(schema))

    val result = DataFrameGenerator.getIpFrequencyByOctet(mockDF,3)

    assert(result.count() == 6)
    assert(result.columns.length == 2)
    val resultAsList = result.collectAsList()
    assert(resultAsList.contains(Row("0.0.0", 1)))
    assert(resultAsList.contains(Row("1.0.0", 2)))
    assert(resultAsList.contains(Row("2.0.0", 3)))
    assert(resultAsList.contains(Row("255.0.0", 2)))
    assert(resultAsList.contains(Row("255.1.0", 1)))
    assert(resultAsList.contains(Row("255.0.1", 1)))
  }

  test("Number of referral by members") {

    val data = Seq(
      Row(1, "b", "b@1.com", 1593968000.toLong, "1.0.0.0", 1001, 1, 1, 1901, 0, 0),
      Row(2, "c", "c@2.com", 1593967000.toLong, "1.0.0.2", 10001, 2, 1, 1900, 0, 0),
      Row(3, "d", "d@3.com", 1569888000.toLong, "2.0.0.0", 10000, 0, 0, 0, 0, 1),
      Row(4, "e", "e@4.com", 1569887000.toLong, "2.0.0.0", 9999, 0, 0, 0, 0, 1),
      Row(5, "f", "f@4.com", 1569886000.toLong, "2.0.0.1", 9999, 0, 0, 0, 0, 2),
      Row(4, "g", "g@4.com", 1369887000.toLong, "255.0.0.0", 9999, 0, 0, 0, 0, 0),
      Row(5, "h", "h@4.com", 1169886000.toLong, "255.0.0.0", 9999, 0, 0, 0, 0, 4),
      Row(4, "i", "i@4.com", 1069887000.toLong, "255.1.0.0", 9999, 0, 0, 0, 0, 4),
      Row(5, "j", "j@4.com", 869886000.toLong, "255.0.1.0", 9999, 0, 0, 0, 0, 4))

    val mockDF = spark.createDataFrame(spark.sparkContext.parallelize(data), StructType(schema))

    val result = DataFrameGenerator.getReferralsByMembers(mockDF)

    assert(result.count() == 3)
    assert(result.columns.length == 2)
    val resultAsList = result.collectAsList()
    assert(resultAsList.contains(Row(1, 2)))
    assert(resultAsList.contains(Row(2, 1)))
    assert(resultAsList.contains(Row(4, 3)))
  }

}
