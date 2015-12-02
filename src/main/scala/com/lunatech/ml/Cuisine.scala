package com.lunatech.ml

import org.apache.spark.{SparkContext, SparkConf}

object Cuisine {

  val trainDataPath: String = "train.json"

  def main(args: Array[String]): Unit = {

    // Prepare Spark context
    val conf = new SparkConf()
      .setAppName("Cuisine")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    FileFormat.convertJSONToLibSVM(sc, trainDataPath)

  }
}












