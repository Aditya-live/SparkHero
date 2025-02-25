package com.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, lower, split}


object WordCountDataset {

  case class Book(value: String)
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FindingByAge")

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    import spark.implicits._

    // Read the text file
    val input = spark.read.text("data/book.txt").as[Book]

    val words = input.select(explode(split($"value", "\\W+")).alias("word")).filter($"word" =!="")

    val lowerCaseWord = words.select(lower($"word").alias("word"))

    val wordCount = lowerCaseWord.groupBy($"word").count()

    val wordCountSorted = wordCount.sort($"count")

    wordCountSorted.show(wordCountSorted.count().toInt)

  }
}
