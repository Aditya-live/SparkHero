package com.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

object TotalSpentByCustomerDataset {
  case class CustomerOrders(cust_id: Int, item_id: Int, amount_spent: Double)
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "TotalSpentByCustomerDataset")

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    // Creating a schema for the input data
    val customerOrderSchema =new StructType()
      .add("cust_id", IntegerType, nullable = true)
      .add("item_id", IntegerType, nullable = true)
      .add("amount_spent", DoubleType, nullable = true)

    import spark.implicits._

    // Read the data from the csv file and convert into a dataset
    val customerOrders = spark.read.schema(customerOrderSchema).csv("data/customer-orders.csv").as[CustomerOrders]

   // Calculate the total spending by each customer
    val totalSpentByCustomer = customerOrders
      .groupBy($"cust_id")
      .agg(round(sum("amount_spent"),2).alias("total_spent"))

   // Sort the dataset based on total spendings
    val totalByCustomerSorted = totalSpentByCustomer.sort($"total_spent".desc)

    totalByCustomerSorted.show()
  }
}
