package main

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  private type GMM = (Array[Double], Array[Double], Array[Double]);

  def printResults(
      arrays: (Array[Double], Array[Double], Array[Double])
  ): Unit = {
    println("Weights: " + arrays._1.mkString(", "))
    println("Means: " + arrays._2.mkString(", "))
    println("Variances " + arrays._3.mkString(", "))
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("Datasets Test")
    conf.setMaster("local[*]")
    val filename = "dataset.txt"
    val sc = new SparkContext(conf)
    val epsilon = 0.001
    val initRdd = sc.textFile(filename).map(_.toDouble).persist()
    printResults(EM(initRdd, 3, epsilon))
    //printResults(EM(initRdd, 10, epsilon))

  }

  private def logLikelihood(
      X: RDD[Double],
      weights: Array[Double],
      means: Array[Double],
      variance: Array[Double]
  ): Double = {
    val K = weights.length
    X.map(x => {
      Math.log(
        (0 until K)
          .map((k) =>
            weights(k) * Math.exp(
              -0.5 * Math.pow(x - means(k), 2) / variance(k)
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

  private def EM(X: RDD[Double], K: Int, epsilon: Double): GMM = {
    var meansVector = X.takeSample(withReplacement = false, num = K)
    val count = X.count()
    val variance = X.variance()
    var varianceVector = Array.fill(K)(variance)
    var weightsVector: Array[Double] = Array.fill(K)(1.0 / K)
    var currentLogLikelihood =
      logLikelihood(X, weightsVector, meansVector, varianceVector)
    var prevLogLikelihood: Double = currentLogLikelihood

    do {
      val gammaRDD = gamma(
        X,
        weights = weightsVector,
        means = meansVector,
        variances = varianceVector
      ).persist()
      val sumGammas: Array[Double] = gammaRDD.reduce((prev, current) =>
        prev
          .zip(current)
          .map(prevZippedCurrent => prevZippedCurrent._1 + prevZippedCurrent._2)
      )
      weightsVector = sumGammas.map(_ / count)
      meansVector = gammaRDD
        .zip(X)
        .map(gammaZippedX => gammaZippedX._1.map(_ * gammaZippedX._2))
        .reduce((prev, current) =>
          prev
            .zip(current)
            .map(prevZippedCurrent =>
              prevZippedCurrent._1 + prevZippedCurrent._2
            )
        )
        .zip(sumGammas)
        .map(sumGammaTimesMeanZippedSumGammas =>
          sumGammaTimesMeanZippedSumGammas._1 / sumGammaTimesMeanZippedSumGammas._2
        )
      varianceVector = gammaRDD
        .zip(X.map(x => meansVector.map(mean => Math.pow(x - mean, 2))))
        .map(gammaZippedXMinusMeansSquared =>
          gammaZippedXMinusMeansSquared._1
            .zip(gammaZippedXMinusMeansSquared._2)
            .map(gammaZippedXMinusMeansSquaredVectors =>
              gammaZippedXMinusMeansSquaredVectors._1 * gammaZippedXMinusMeansSquaredVectors._2
            )
        )
        .reduce((a, b) => a.zip(b).map(t => t._1 + t._2))
        .zip(sumGammas)
        .map(t => t._1 / t._2)

      prevLogLikelihood = currentLogLikelihood
      println("Intermediate results: ")
      printResults((weightsVector, meansVector, varianceVector))
      currentLogLikelihood = logLikelihood(
        X,
        weights = weightsVector,
        means = meansVector,
        variance = varianceVector
      )

    } while ((currentLogLikelihood - prevLogLikelihood) > epsilon)
    (weightsVector, meansVector, varianceVector)
  }
}
