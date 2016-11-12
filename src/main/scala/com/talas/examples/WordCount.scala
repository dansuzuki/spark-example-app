package com.talas.examples


import org.apache.spark._

object WordCount extends App {


    val conf = new SparkConf
    conf.setAppName("Word Count").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val textData: List[String] = (1 to 10000).toList.map(n => {
      "the quick brown fox jumps over the lazy dog"
    })

    val rdd = sc.parallelize(textData)
    rdd.flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collect
      .foreach(println)

    sc.stop
}
