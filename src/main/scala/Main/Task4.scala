package Main

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Task4 {
  def main(args: Array[String]): Unit = {

    /*
    !!! use copy command in cmd/shell on a .csv file and copy it into my_stream_data in the root directory
    No sure why, but the basic copy/paste (without cmd) is ignored.
    Files previously located in the my_stream_data will be also ignored.
     */
    val sparkConf = new SparkConf()
      .setAppName("Spark Application")
      .setMaster("local[3]")

    val streamingContext = new StreamingContext(sparkConf, Seconds(1)) // Sliding Duration 1
    val ds = streamingContext
      .textFileStream("my_stream_data")
      .window(Seconds(2)) // Window Duration 1

    val task_4_ds = ds
      .map(_.split(';'))
      .filter(line => line(0).equals("0") && line(5).equals("0"))
      .map(line => (line(1), 1))
      .reduceByKey(_ + _)
      .print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
