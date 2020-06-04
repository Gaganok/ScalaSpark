package Main

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Task5 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Spark Application")
      .setMaster("local[3]")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate();

    val data = spark.read.textFile("wiki_dataset").rdd

    val task5 = data
      .map(_.split(";"))
      .map(line => (line(0) + "_" + line(2), (line(1), Integer.parseInt(line(3)))))
      .reduceByKey((x1, x2) => {if(x1._2 < x2._2) x2 else x1})
      .sortBy(_._2._2, false)

    println(task5.collect().toList)

  }
}
