package com.dallinhuff.sparkjobs

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.StdIn.readLine

given Ordering[(String, Int)] = Ordering[Int].on(_._2)

def setupSpark(name: String): SparkContext =
  val spark = SparkSession
    .builder()
    .appName(name)
    .master("local[*]")
    .getOrCreate()

  val sparkContext = spark.sparkContext
  sparkContext.setLogLevel("ERROR")
  sparkContext

def getWordCounts(
    sparkContext: SparkContext,
    filePath: String,
    numPartitions: Int = 8
): RDD[(String, Int)] =
  sparkContext
    .textFile(filePath, numPartitions)
    .flatMap: line =>
      line.replaceAll("[^a-zA-Z0-9\\s]", "")
        .toLowerCase
        .split("\\s+")
    .filter(_.nonEmpty)
    .map((_, 1))
    .reduceByKey(_ + _)

@main def runWordCounter(): Unit =
  val sparkContext = setupSpark("WordCount")

  println(
    """
      |  --------------------------
      | /      WORD COUNTER      /
      |--------------------------
      |""".stripMargin)

  while true do
    val bookTitle = readLine("Enter book title: ")
    val bookPath = readLine("Enter book path: ")
    val numWords =
      readLine("Enter number of words to show: ")
        .toIntOption
        .getOrElse(10)

    val wordCounts = getWordCounts(sparkContext, bookPath)
    val topWordCounts = wordCounts.top(numWords)

    println(s"\nThe ${topWordCounts.length} most common words in $bookTitle are:")
    
    for (word, count) <- topWordCounts do
      println(s"  - $word ($count)")
      
    println()
