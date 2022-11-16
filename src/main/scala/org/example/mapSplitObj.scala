package org.example

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col,split}

object mapSplitObj extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("MapSplitProgram")
    .getOrCreate()

  import spark.implicits._

  val data = Seq(
    ("Kate Winslet, Eternal Sunshine of spotless mind, Actor")
    , ("Gasper noè, Climax, Director")
    , ("Lars Von Trier, Dogville, Director")
    , ("Marion Cotilard, Two day and one night, Actor")
    , ("Sam Smith, Unholy, Singer")
    , ("Burno Mars, Taking to the moon, Singer")
  ).toDF("raw_data")

  data.show(false)

  /* Output 1:
  +------------------------------------------------------+
  |raw_data                                              |
  +------------------------------------------------------+
  |Kate Winslet, Eternal Sunshine of spotless mind, Actor|
  |Gasper noè, Climax, Director                          |
  |Lars Von Trier, Dogville, Director                    |
  |Marion Cotilard, Two day and one night, Actor         |
  |Sam Smith, Unholy, Singer                             |
  |Burno Mars, Taking to the moon, Singer                |
  +------------------------------------------------------+
  */

  val df = data.withColumn("Name", split(col("raw_data"), ",").getItem(0))
    .withColumn("Movie", split(col("raw_data"), ",").getItem(1))
    .withColumn("Profession", split(col("raw_data"), ",").getItem(2))
    .drop("raw_data")

  df.show(false)

  /* Output 2:
  +---------------+----------------------------------+----------+
  |Name           |Movie                             |Profession|
  +---------------+----------------------------------+----------+
  |Kate Winslet   | Eternal Sunshine of spotless mind| Actor    |
  |Gasper noè     | Climax                           | Director |
  |Lars Von Trier | Dogville                         | Director |
  |Marion Cotilard| Two day and one night            | Actor    |
  |Sam Smith      | Unholy                           | Singer   |
  |Burno Mars     | Taking to the moon               | Singer   |
  +---------------+----------------------------------+----------+
  */
}
