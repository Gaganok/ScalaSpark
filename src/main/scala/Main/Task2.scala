package Main

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Task2 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Spark Application")
      .setMaster("local[2]")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate();

    import spark.implicits._

    val data = spark.read.textFile("my_dataset/*.csv").rdd
      .map(_.split(';'))
      .persist()

    val fitz_data = data
          .filter(line => line(0).equals("0") && line(5).equals("0") && line(1).equalsIgnoreCase("Fitzgerald's Park"))

    val total_runouts = fitz_data
      .map(line => (line(4).substring(0,10), line(4).substring(11,19)))
      .count()

    val task2 = fitz_data
      .filter(line => line(0).equals("0") && line(5).equals("0") && line(1).equalsIgnoreCase("Fitzgerald's Park"))
      .map(line => (dayOfWeek(line(4).substring(0, 10)) + "_" + line(4).substring(11, 13), 1))
      .reduceByKey(_ + _)
      .map(tuple => (tuple._1, (tuple._2, percent(tuple._2, total_runouts))))
      .sortBy(record => record._2._1, false)

    println(task2.collect().toList)
  }

  def percent(a: Float, b:Float): Float = {
    a / b *100
  }

  def dayOfWeek(date: String): String = {
    val df = DateTimeFormatter.ofPattern("dd-MM-yyyy")
    LocalDate.parse(date,df).getDayOfWeek().toString
  }
}
