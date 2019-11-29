package com.fenglex.scala.simple

import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @Author: haifeng
 * @Date: 2019/11/29 11:33
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("WordCount").getOrCreate()
    val context = spark.sparkContext
    val lines = context.textFile("D:\\env\\code\\spark-start\\document\\word.txt")
    val tuples = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).filter(_._1.nonEmpty).sortBy(_._2, false).take(10)
    tuples.foreach(println(_))
  }
}
