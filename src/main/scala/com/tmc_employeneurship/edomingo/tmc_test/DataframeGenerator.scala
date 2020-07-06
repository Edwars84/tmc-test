package com.tmc_employeneurship.edomingo.tmc_test

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataFrameGenerator {

  private val takeProviderUDF = udf( (email: String) => email.split("@").last)
  private val takeOctetsUDF = udf( (ip: String, n_octet: Int) => ip.split("\\.").take(n_octet).mkString("."))

  /**
   * Generate a query to obtain a most birthdays on 1 day
   * @param df
   * @return
   */
  def getMostBirthdaysOn1Day(df: DataFrame) = {
    df.groupBy(col("bday_month"), col("bday_day")).agg(count(col("member_id")).as("num_bdays"))
      .orderBy(desc("num_bdays"))
      .limit(1)
      .select("bday_day", "bday_month", "num_bdays")
  }

  //def getLeastBirthdays11Months(df: DataFrame) = {
    // I'm very sorry but I don't understand this question
  //}

  /**
   * Email providers with more than 10k posts
   * @param df
   * @return
   */
  def getEmailProvidersMore10kPosts(df: DataFrame) = {

    df.withColumn("email_provider", takeProviderUDF(col("email")))
      .groupBy(col("email_provider")).agg(sum(col("posts")).as("num_posts"))
      .filter(col("num_posts").gt(10000)).select(col("email_provider"))
  }

  /**
   * Post by email providers
   * @param df
   * @return
   */
  def getNumPostByEmailProvider(df: DataFrame) = {
    df.withColumn("email_provider", takeProviderUDF(col("email")))
      .groupBy(col("email_provider")).agg(sum(col("posts")).as("num_posts"))
      .select(col("email_provider"), col("num_posts"))
  }

  /**
   * Year/s with max sign ups
   * @param df
   * @return
   */
  def getYearsMaxSignUps(df: DataFrame) = {
    val dfAux = df.withColumn("joined_date", from_unixtime(col("joined"), "yyyy-MM-dd"))
      .groupBy(year(col("joined_date")).as("year")).agg(count("member_id").as("num_sign_ups")).persist()
    val maxSignUpDf = dfAux.select("num_sign_ups").orderBy(desc("num_sign_ups")).limit(1).withColumnRenamed("num_sign_ups", "max_num_sign_ups")

    dfAux.join(maxSignUpDf, dfAux.col("num_sign_ups").equalTo(maxSignUpDf.col("max_num_sign_ups"))).drop("max_num_sign_ups")
  }

  /**
   * Class C IP address frequency by 1 st octet
   * Frequency of IP address based on first 3 octets
   * @param df
   * @param n_octet
   * @return
   */
  def getIpFrequencyByOctet(df: DataFrame, n_octet: Int) = {
    df.withColumn("partial_ip", takeOctetsUDF(col("ip_address"), lit(n_octet)))
      .groupBy("partial_ip").agg(count(col("member_id")).as("frequency"))
      .select("partial_ip", "frequency")
  }

  /**
   * Number of referral by members
   * @param df
   * @return
   */
  def getReferralsByMembers(df: DataFrame) = {
    df.filter(col("referred_by").gt(0))
      .groupBy("referred_by").agg(count("member_id").as("number_of_referals"))
      .select(col("referred_by").as("member_id"), col("number_of_referals"))
  }
}
