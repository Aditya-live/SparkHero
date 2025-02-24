package com.spark

import org.apache.spark._
import org.apache.log4j._
import scala.math.min
import scala.math.max

//Find the minimum weather in degree Farhenhiet by weather station
object MinTemperatures {

  def parseLine(line:String)={
    val field = line.split(",")
    val station = field(0)
    val entryType = field(2)
    val temp = field(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f

    (station, entryType, temp)
  }

  def main(args: Array[String]):Unit={
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")

    // Read each line of input data
    val lines = sc.textFile("data/1800.csv")

    val parsedLines = lines.map(parseLine)

    val minTemp = parsedLines.filter(x=>x._2=="TMAX")

    val stationTemp = minTemp.map(x=>(x._1, x._3))

    val minTempOfVariousStations = stationTemp.reduceByKey((x,y)=>max(x,y))

    val results = minTempOfVariousStations.collect()

    for(result <- results.sorted){
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$station minimum temperature: $formattedTemp")
    }
  }
}
