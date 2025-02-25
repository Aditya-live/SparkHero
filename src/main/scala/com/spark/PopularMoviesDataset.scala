package com.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object PopularMoviesDataset {

  final case class Movie(movieId: Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PopularMoviesDataset")

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    import spark.implicits._

    val movieSchema = new StructType()
      .add("userId", IntegerType, nullable = true)
      .add("movieId", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    // Load up movie data as dataset
    val movieDataset = spark.read.option("sep","\t").schema(movieSchema).csv("data/ml-100k/u.data").as[Movie]
    movieDataset.show()

    //Sorting movies by popularity (Assuming the popularity means movie which has most ratings)
    val topMovieIds = movieDataset.groupBy("movieId").count().orderBy(desc("count"))

    // Showing top 10 most popular movies
    topMovieIds.show(10)

    // Stop the session
    spark.stop()
  }
}
