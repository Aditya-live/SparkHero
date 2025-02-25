package com.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.spark.sql.functions.{col, udf}

import scala.io.{Codec, Source}
object PopularMoviesNicerDataset {
  case class Movies(userId: Int, movieId: Int, rating: Int, timestamp: Long)

  def loadMovieNames(): Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec: Codec = Codec("ISO-8859-1") // This is the current encoding of u.item, not UTF-8

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames: Map[Int, String] = Map()

    //
    val lines = Source.fromFile("data/ml-100k/u.item")
    for (line <- lines.getLines()){
      val field = line.split("|")
      if (field.length >1){
        movieNames += (field(0).toInt -> field(1))
      }
    }
    lines.close()

    movieNames
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PopularMoviesNicerDataset")

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    val nameDict = spark.sparkContext.broadcast(loadMovieNames())
    import spark.implicits._

    val movieSchema = new StructType()
      .add("userId", IntegerType, nullable = true)
      .add("movieId", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    val movies = spark.read.option("sep","\t").schema(movieSchema).csv("data/ml-100k/u.data").as[Movies]

    val moviesCount = movies.groupBy("movieId").count()

    // Declaring an "anonymous function" in Scala
    val lookupName: Int => String = (movieId: Int) => nameDict.value(movieId)

    // Wrapping it up with Udf
    val lookupNameUDF = udf(lookupName)

    // Adding a new column with movieTitles values
    val moviesWithNames = moviesCount.withColumn("movieTitle", lookupNameUDF(col("movieId")))
    moviesWithNames.show()

    // Sort the results
    val sortedMoviesWithName = moviesCount.sort("count")

    // Show the results without truncating
    sortedMoviesWithName.show(sortedMoviesWithName.count.toInt, truncate = false)
  }
}
