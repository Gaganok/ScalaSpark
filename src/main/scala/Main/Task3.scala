package Main

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.{count, first, lag, sum}

object Task3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Spark Application")
      .setMaster("local[2]")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate();

    import spark.implicits._

    val schema = new StructType()
      .add(StructField("status", IntegerType, true))
      .add(StructField("station", StringType, true))
      .add(StructField("longitude", FloatType, true))
      .add(StructField("latitude", FloatType, true))
      .add(StructField("dateStatus", TimestampType, true))
      .add(StructField("bikesAvailable", IntegerType, true))
      .add(StructField("docksAvailable", IntegerType, true))

      val df = spark.read
        .schema(schema)
        .format("csv")
        .option("delimiter",";")
        .option("header", "false")
        //.option("dateFormat", "yyyy-MM-dd hh:mm:ss")
        .option("timestampFormat", "dd-MM-yyyy hh:mm:ss")
        .load("my_dataset/*.csv");


      val w = Window.orderBy($"dateStatus")
      val task3 = df
        .filter($"station" === "Fitzgerald's Park" && $"status" === 0 && $"bikesAvailable" === 0)
        .withColumn("Lag",  lag($"dateStatus" , 1).over(w))
        .withColumn("isNew", ($"dateStatus".cast("long") - $"lag".cast("long")) / 60 > 5)
        .na.fill(true, Seq("isNew"))
        .withColumn("group_id", sum($"isNew".cast("int")).over( w))
        .groupBy("group_id")
        .agg(first($"dateStatus").alias("dateStatus"), count("*").alias("nb"))
        task3.show()
  }
}
