package com.dallinhuff.sparkjobs

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.*

def readData(): DataFrame =
  val spark = SparkSession
    .builder()
    .appName("CountiesCovid")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val schemaString =
    """
      |date DATE,
      |county STRING,
      |state STRING,
      |fips INT,
      |cases INT,
      |deaths INT
      """.stripMargin

  val dateWindowSpec = Window
    .partitionBy("county", "state")
    .orderBy(col("date").desc)

  spark.read
    .schema(schemaString)
    .csv("src/main/resources/us-counties.csv")
    .withColumn("row_number", row_number().over(dateWindowSpec))
    .filter(col("row_number") === 1)
    .drop("row_number")

def getMaxBy(df: DataFrame, colName: String): (String, Long) =
  val max =
    df.groupBy("county")
      .agg(sum(colName).alias(colName))
      .reduce: (r1, r2) =>
        if r1.getAs[Long](colName) > r2.getAs[Long](colName) then r1
        else r2
  (max.getAs[String]("county"), max.getAs[Long](colName))

def getTotalDeathsIn(
    df: DataFrame,
    county: String,
    state: String
): Long =
  df.where(s"county = '$county' AND state = '$state'")
    .groupBy("county")
    .agg(sum("deaths").alias("deaths"))
    .first()
    .getAs[Long]("deaths")

def getDeathRatesBy(df: DataFrame, colName: String): DataFrame =
  df.groupBy(colName)
    .agg(
      sum("deaths").alias("deaths"),
      sum("cases").alias("cases")
    )
    .withColumn("death_rate", col("deaths") / col("cases"))
    .select(colName, "death_rate")
    .na
    .drop()

@main def run(): Unit =
  val df = readData()

  val maxDeaths = getMaxBy(df, "deaths")
  println(s"County with most deaths: $maxDeaths")

  val maxCases = getMaxBy(df, "cases")
  println(s"County with most cases: $maxCases")

  val totalDeathsInUtahCounty =
    getTotalDeathsIn(df, "Utah", "Utah")
  println(s"Deaths in Utah County: $totalDeathsInUtahCounty")

  val top5DeathRates = getDeathRatesBy(df, "state")
    .orderBy(col("death_rate").desc)
    .take(5)
    .map: r =>
      (r.getAs[String]("state"), r.getAs[Double]("death_rate"))
    .mkString("\n  - ", "\n  - ", "")
  println(s"States with the highest death rates: $top5DeathRates")

  val countiesWithNoDeaths = getDeathRatesBy(df, "county")
    .where(col("death_rate") === 0)
    .take(15)
    .map(_.getAs[String]("county"))
    .mkString("\n  - ", "\n  - ", "")
  println(s"Counties with cases but no reported deaths: $countiesWithNoDeaths")
