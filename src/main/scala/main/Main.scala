package main

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("Datasets Test")
    conf.setMaster("local[*]")
    val filename = "dataset.txt"
    val sc = new SparkContext(conf)
    val initRdd = sc.textFile(filename).map(_.toDouble).persist()
    EM(initRdd, 5)

  }

  private def EM(X: RDD[Double], K:Int): Unit = {
    val count = X.count()
    val sample = X.takeSample(withReplacement = false, num = K)
    val mean = X.mean()
    val variance = X.variance()
    val stdVector = Array.fill(K)(variance)
    val weightsVector = Array.fill(K)(1/K)
    println("Count: " + count)
    println("Mean: " + mean)
    println("Variance: "+ variance)
    println("Std: "+ stdVector.mkString("Array(", ", ", ")"))
    println("Sample: " + sample.mkString("Array(", ", ", ")"))

  }
}
