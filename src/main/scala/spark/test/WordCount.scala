package spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wordcount")
      //.setMaster("local[*]")
    // 如果这里指定为local，那么可以使用本地环境运行
    val sc = new SparkContext(conf)

    val input: RDD[String] = sc.textFile(args(0))
    val words: RDD[String] = input.flatMap(_.split(" "))
    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
    val reduced: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    sorted.saveAsTextFile(args(1))

    sc.stop()
  }
}
