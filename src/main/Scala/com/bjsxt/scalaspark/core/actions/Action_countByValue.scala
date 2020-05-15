package com.bjsxt.scalaspark.core.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * countByValue
  * 计数RDD中相同的value 出现的次数,不必须是K,V格式的RDD
  */
object Action_countByValue {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("countByKey").setMaster("local")
      val sc = new SparkContext(conf)
      val rdd = sc.makeRDD(List[(String,Integer)](("a",1),("a",1),("a",1000),("b",2),("b",200),("c",3),("c",3)))
      val result: collection.Map[(String, Integer), Long] = rdd.countByValue()
      result.foreach(print)
  }
}
