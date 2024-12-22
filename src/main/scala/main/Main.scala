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
    EM(initRdd, 10)

  }

  private def logLikelihood(
      X: RDD[Double],
      weights: Array[Double],
      sample: Array[Double],
      variance: Array[Double]
  ): Double = {
    val K = weights.length
    X.map(x => {
      Math.log(
        (0 until K)
          .map((k) =>
            weights(k) * Math.exp(
              -0.5 * Math.pow(x - sample(k), 2) / variance(k)
            ) / Math.sqrt(2 * Math.PI * variance(k))
          )
          .sum
      )
    }).sum()
  }

  private def gamma(
      X: RDD[Double],
      weights: Array[Double],
      means: Array[Double],
      variances: Array[Double]
  ): RDD[Array[Double]] = {
    X.map(x => {
      val K = weights.length
      val nominators = (0 until K).map(k =>
        weights(k) * Math.exp(
          -0.5 * Math.pow(x - means(k), 2) / variances(k)
        ) / Math.sqrt(2 * Math.PI * variances(k))
      )
      val denominator = nominators.sum
      nominators.map(n => n / denominator).toArray
    })
  }

  private def EM(X: RDD[Double], K: Int): Unit = {
    var meansVector = X.takeSample(withReplacement = false, num = K)
    val count = X.count()
    val variance = X.variance()
    val varianceVector = Array.fill(K)(variance)
    var weightsVector: Array[Double] = Array.fill(K)(1 / K)
    val currentLogLikelihood =
      logLikelihood(X, weightsVector, meansVector, varianceVector)
    val prevLogLikelihood = currentLogLikelihood

    do {
      val gammaRDD = gamma(
        X,
        weights = weightsVector,
        means = meansVector,
        variances = varianceVector
      ).persist()
      val sumXs = gammaRDD.reduce((a, b) => a.zip(b).map((t) => t._1 + t._2))
      weightsVector = sumXs.map(_ / count)
      meansVector = gammaRDD
        .zip(X)
        .map((t) => t._1.map(_ * t._2))
        .reduce((a, b) => a.zip(b).map((t) => t._1 + t._2))
        .zip(sumXs)
        .map((t) => t._1 / t._2)


    } while (false)
    // println(s"Means: " + meansVector.toString())
    // println(s"Variance: " + varianceVector.toString())
    // println(s"Weights: " + weightsVector.toString())
    println("logLikelihood: " + currentLogLikelihood)
  }
}
