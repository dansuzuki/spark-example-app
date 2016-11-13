package com.talas.examples


import org.apache.spark._

object WordCount extends App {

    val conf = new SparkConf
    conf.setAppName("Word Count").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val started = System.currentTimeMillis
    val rdd = sc.textFile(args(0))
    rdd.flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(kv => (kv._1 + "\t" + kv._2))
      .repartition(1)
      .saveAsTextFile(args(1))
    val finished = System.currentTimeMillis

    sc.stop
    println("Time taken: " + (finished - started))
}
