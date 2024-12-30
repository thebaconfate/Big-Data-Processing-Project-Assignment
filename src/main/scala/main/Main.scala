package main

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, PrintWriter}

object Main {
  private type GMM = (Array[Double], Array[Double], Array[Double]);

  private def printResults(
                            arrays: (Array[Double], Array[Double], Array[Double])
                          ): String = {
    s"Weights: ${arrays._1.mkString(", ")}\n" +
      s"Means:  ${arrays._2.mkString(", ")}\n" +
      s"Variances ${arrays._3.mkString(", ")}"
  }

  private def formatNanos(nanos: Long): String = {
    var ms = nanos / 1_000_000
    val ns = nanos % 1_000_000
    var seconds = ms / 1_000
    ms = ms % 1_000
    var minutes = seconds / 60
    seconds = seconds % 60
    val hours = minutes / 60
    minutes = minutes % 60
    f"$hours%02dh $minutes%02dm $seconds%02ds $ms%03dms $ns%09dns"
  }

  private def iterate(initRdd: RDD[Double], k: Int, e: Double, writer: PrintWriter): Unit = {
    var time: Long = 0
    var weights: Array[Double] = Array.fill(k)(0)
    var means: Array[Double] = Array.fill(k)(0)
    var variances: Array[Double] = Array.fill(k)(0)
    for (_ <- 0 until 100) {
      val startTime = System.nanoTime()
      val r = EM(initRdd, k, e)
      time = time + (System.nanoTime() - startTime)
      weights = weights.zip(r._1).map(e => e._1 + e._2)
      means = means.zip(r._2).map(e => e._1 + e._2)
      variances = variances.zip(r._3).map(e => e._1 + e._2)
    }
    time = time / 100
    weights = weights.map(_ / 100)
    means = means.map(_ / 100)
    variances = variances.map(_ / 100)
    writer.println(formatNanos(time))
    writer.println(weights.mkString(", "))
    writer.println(means.mkString(", "))
    writer.println(variances.mkString(", "))
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("Datasets Test")
    conf.setMaster("local[*]")
    val filename = "dataset.txt"
    //val filename = "/data/bigDataSecret/dataset-small.txt"
    val sc = new SparkContext(conf)
    val epsilon = 0.01
    val initRdd =
      sc.textFile(filename)
        .map(_.toDouble)
        .persist() // TODO: (Un)Comment this persist to test performance
    val startTime = System.nanoTime()
    val results = EM(initRdd, 5, epsilon)
    val endTime = System.nanoTime()
    println("Elapsed time: " + formatNanos(endTime - startTime))
    println(printResults(results))
    val writer = new PrintWriter(new File("logs.txt"))
    try {
      writer.println("Elapsed time: " + formatNanos(endTime - startTime))
      writer.println(printResults(results))
    } finally {
      writer.close()
    }
    val k3e01 = new PrintWriter(new File("k3e01.txt"))
    val k5e01 = new PrintWriter(new File("k5e01.txt"))
    val k3e001 = new PrintWriter(new File("k3e001.txt"))
    val k5e001 = new PrintWriter(new File("k5e001.txt"))
    iterate(initRdd, 3, 0.01, k3e01)
    iterate(initRdd, 5, 0.01, k5e01)
    iterate(initRdd, 3, 0.001, k3e001)
    iterate(initRdd, 5, 0.001, k5e001)
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
          .map(k =>
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
                   ): RDD[Array[Double]] = X.map(x => {
    val K = weights.length
    val nominators = (0 until K).map(k =>
      weights(k) * Math.exp(
        -0.5 * Math.pow(x - means(k), 2) / variances(k)
      ) / Math.sqrt(2 * Math.PI * variances(k))
    )
    val denominator = nominators.sum
    nominators.map(n => n / denominator).toArray
  })

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
      ).persist() // TODO: (Un)Comment this persist to test performance
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
        .reduce((prev, current) =>
          prev
            .zip(current)
            .map(prevZippedCurrent =>
              prevZippedCurrent._1 + prevZippedCurrent._2
            )
        )
        .zip(sumGammas)
        .map(t => t._1 / t._2)

      prevLogLikelihood = currentLogLikelihood
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
