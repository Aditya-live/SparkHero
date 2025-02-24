package com.spark

import org.apache.spark._
import org.apache.log4j._
import scala.math.min
import scala.math.max

object WordCount {
  def main(args: Array[String]):Unit= {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")

    // Read each line of input data
    val lines = sc.textFile("C:/Users/adity/Desktop")

    val words = lines.flatMap(x=>x.split("\\W+"))

    val lowerCase = words.map(x=>x.toLowerCase())

    val wordCount =  lowerCase.countByValue()
//    wordCount.toSeq.sortBy(_._2).foreach(println)
  }
}
