package com.spark

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.round


object FriendsByAgeDataset {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FindingByAge")

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    import spark.implicits._

    val friends = spark.read.option("inferSchema","true").option("header","true").csv("data/fakefriends.csv").as[FriendsDetails]

    val friendsByAge = friends.select("age","friends").groupBy("age").avg("friends").sort("age")

    //To have a better formatting
    friends.select("age","friends").groupBy("age").agg(round(avg("friends"),2)).alias("friends_avg").show()
  }
  }
case class FriendsDetails(id: Int, name:String, age: Int, friends:Long)
