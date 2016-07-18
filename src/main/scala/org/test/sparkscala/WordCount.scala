package org.test.sparkscala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
    
    val test = sc.textFile("./data/food.txt")
    
    test.flatMap { line =>
      line.split(" ")
    }
    .map { word =>
      (word, 1)
    }
    .saveAsTextFile("./output/foodcount.")
  }
}