package com.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._

object MostPopularSuperheroDataset {

  case class SuperHeroName(id:Int, name: String)
  case class SuperHero(value: String)

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("MostPopularSuperhero")
      .master("local[*]")
      .getOrCreate()

    // Create schema when reading Marvel-names.txt
    val superHeroNameSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    //Build up a hero ID -> name Dataset
    import spark.implicits._

    val names = spark.read
      .schema(superHeroNameSchema)
      .option("sep"," ")
      .csv("data/Marvel-names.txt")
      .as[SuperHeroName]


    val lines= spark.read
      .text("data/Marvel-graph.txt")
      .as[SuperHero]

    val connections = lines.withColumn("id", split(col("value")," ")(0))
      .withColumn("connections", size(split(col("value"), " ")) -1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    val mostPopularSuperhero = connections
      .sort($"connections".desc)
      .first()

    val mostPopularName = names
      .filter($"id"===mostPopularSuperhero(0))
      .select("name")
      .first()

    println(s"${mostPopularName(0)} is the most popular superhero with ${mostPopularSuperhero(1)} co-appearances.")
  }
}
