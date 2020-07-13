package com.xwql.scalaspark.streaming

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object StructuredStreamingTest {
  val spark: SparkSession = SparkSession.builder().getOrCreate()
  val df: DataFrame = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
//  df.createOrReplaceTempView("updates")
//  val frame: DataFrame = spark.sql("select count(*) from updates")// returns another streaming DF
//  df.isStreaming
//  df.printSchema()
//  val userSchema = new StructType().add("name", "string").add("age", "integer")
//  val csvDF = spark
//    .readStream
//    .option("sep", ";")
//    .schema(userSchema)      // Specify schema of the csv files
//    .csv("/path/to/directory")    // Equivalent to format("csv").load("/path/to/directory")
//  df.select("").filter("").where("").groupBy("").count()
  //cdf:DataFrame = csvDF.as[XXXX]
  import spark.implicits._
  val words = df.as[String].flatMap(_.split(" "))
  // Generate running word count
  val wordCounts = words.groupBy("value").count()


  import org.apache.spark.sql.functions._   //引入window方法
  val windowedCounts1: DataFrame = words
    .withWatermark("timestamp", "10 minutes")
    .groupBy(
      window($"timestamp", "10 minutes", "5 minutes"),
      $"word")
    .count()
  //val words = csvDF // streaming DataFrame of schema { timestamp: Timestamp, word: String }
  // Group the data by window and word and compute the count of each group
  // Start running the query that prints the running counts to the console
  windowedCounts1.writeStream
      .outputMode("complete")
      .format("console")
     .queryName("wordTable")
      .start()

}
