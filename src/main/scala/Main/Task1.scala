package Main

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Task1 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Spark Application")
      .setMaster("local[2]")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate();

    val data = spark.read.textFile("my_dataset/*.csv").rdd
      .map(_.split(';'))
      .persist()

    val task1 = data
      .filter(line => line(0).equals("0") && line(5).equals("0"))
      .map(line => (line(1), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    println(task1.collect().toList)
  }
}
