package org.example

import org.apache.spark.sql.SparkSession

object mapCombineObj extends App{

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("mapCombineProgram")
    .getOrCreate

  import spark.implicits._

  val data = Seq(
      ("Ajay", "Rahul", "", 117000, "London")
    , ("Kate", "Hudson", "", 501004, "Ohio")
    , ("Adele", "Helene", "Celine", 220014, "Lyon")
    , ("Katie", "Florence", "M", 10041, "Westminster")
  ) toDF("First Name", "Last Name", "Sur Name", "Salary", "City")
  data.show()

  /* Output 1:
    +----------+---------+--------+------+-----------+
    |First Name|Last Name|Sur Name|Salary|       City|
    +----------+---------+--------+------+-----------+
    |      Ajay|    Rahul|        |117000|     London|
    |      Kate|   Hudson|        |501004|       Ohio|
    |     Adele|   Helene|  Celine|220014|       Lyon|
    |     Katie| Florence|       M| 10041|Westminster|
    +----------+---------+--------+------+-----------+
  */

  val df = data.map(x => {
    val full_Name = x.getString(0) + " " + x.getString(1) + " " + x.getString(2)
    (full_Name, x.getInt(3), x.getString(4))
  }).toDF("Full Name", "Salary", "City")
  df.show()

  /* Output 2:
    +-------------------+------+-----------+
    |          Full Name|Salary|       City|
    +-------------------+------+-----------+
    |        Ajay Rahul |117000|     London|
    |       Kate Hudson |501004|       Ohio|
    |Adele Helene Celine|220014|       Lyon|
    |   Katie Florence M| 10041|Westminster|
    +-------------------+------+-----------+
  */
}
