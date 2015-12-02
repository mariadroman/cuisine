package com.lunatech.ml

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

/**
 * Transform file format into MLLib readable format
 */
object FileFormat {
  /**
   * Read multiline JSON file and convert to LibSVM format
   * JSON Example:
   * [
   *  {
   *    "id": 10259,
   *    "cuisine": "greek",
   *    "ingredients": [
   *      "romaine lettuce",
   *      "black olives",
   *      "grape tomatoes",
   *      "garlic",
   *      "pepper",
   *      "purple onion"
   *     ]
   *   },
   *   ...
   * ]
   *
   * LibSVM Example:
   *  1 1:1 2:1 3:1 4:1 5:1 6:1 7:1 8:1 9:1
   *  2 10:1 11:1 12:1 13:1 14:1 15:1 16:1 17:1 18:1 19:1 20:1
   *  3 5:1 12:1 16:1 21:1 22:1 23:1 24:1 25:1 26:1 27:1 28:1 29:1
   *  4 12:1 20:1 30:1 31:1
   *  4 12:1 19:1 28:1 30:1 32:1 33:1 34:1 35:1 36:1 37:1 38:1 39:1 40:1 41:1 42:1 43:1 44:1 45:1 46:1 47:1
   *
   * @param sc
   * @param path origin JSON file
   * @return output LibSVM path
   */
  def convertJSONToLibSVM (sc: SparkContext, path: String): String = {

    val output = "training"

    // Read multiline json file
    val rawData: RDD[(LongWritable, Text)] =
      sc.newAPIHadoopFile[LongWritable, Text, CustomLineInputFormat](path)

    case class Recipe(id: Int, cuisine: String, ingredients: List[String])
    val recipes: List[Recipe] = rawData.map(x => parse(x._2.toString)).map(
      json => {
        implicit lazy val formats = DefaultFormats
        val id = (json \ "id").extract[Int]
        val cuisine = (json \ "cuisine").extract[String]
        val ingredients = (json \ "ingredients").extract[List[String]]
        Recipe(id, cuisine, ingredients)
      }
    ).collect().toList

    val ingredients = recipes.flatMap(_.ingredients).toSet.toIndexedSeq
    val cuisines = recipes.map(_.cuisine).toSet.toIndexedSeq

    val lines = recipes.map { x =>
      (cuisines.indexOf(x.cuisine) + 1).toString +
        " " +
        x.ingredients
          .map(ingredient => ingredients.indexOf(ingredient) + 1)
          .toSet.toSeq.sorted
          .map(_.toString + ":1").mkString(" ")
    }

    scala.tools.nsc.io.File(output).writeAll(lines.mkString("\n"))
    output
  }
}
