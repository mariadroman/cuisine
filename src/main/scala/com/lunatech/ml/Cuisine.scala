package com.lunatech.ml

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.Map

object Cuisine {

  val trainDataPath: String = "train.json"

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("Cuisine")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rawData: RDD[(LongWritable, Text)] =
      sc.newAPIHadoopFile[LongWritable, Text, CustomLineInputFormat](trainDataPath)

    case class Recipe(id: Int, country: String, ingredients: List[String])

    val recipes: List[Recipe] = rawData.map(x => parse(x._2.toString)).map(
      json => {
        implicit lazy val formats = DefaultFormats
        val id = (json \ "id").extract[Int]
        val cuisine = (json \ "cuisine").extract[String]
        val ingredients = (json \ "ingredients").extract[List[String]]
        Recipe(id, cuisine, ingredients)
      }
    ).collect().toList

    var ingredientMap = Map[String, Int]()
    var countryMap = Map[String, Int]()
    var count = 0

    def incrCount: Int = {
      count += 1
      count
    }

    //Set a value for each different ingredient
    recipes.flatMap(_.ingredients).map(x => ingredientMap.getOrElseUpdate(x, incrCount))
    println("Total ingredients: " + ingredientMap.size)

    count = 0
    //Set a value for each different country
    recipes.map(_.country).map(x => countryMap.getOrElseUpdate(x, incrCount))

    val lines = recipes.map { x =>
      countryMap.getOrElse(x.country,0).toString +
        " " +
        x.ingredients.map(i => ingredientMap.getOrElse(i,0)).toSeq.sortBy(x => x).map(_.toString + ":1").mkString(" ")
    }

    scala.tools.nsc.io.File("training.txt").writeAll(lines.mkString("\n"))

  }
}


