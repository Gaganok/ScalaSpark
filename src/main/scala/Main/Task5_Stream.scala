package Main

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object Task5_Stream {
  def main(args: Array[String]): Unit = {

    /*
    !!! use copy command in cmd/shell on a .csv file and copy it into my_stream_data in the root directory
    No sure why, but the basic copy/paste (without cmd) is ignored.
    Files previously located in the my_stream_data will be also ignored.
     */
    val sparkConf = new SparkConf()
      .setAppName("Spark Application")
      .setMaster("local[3]")

    val streamingContext = new StreamingContext(sparkConf, Seconds(15)) // Sliding Duration 1

    val ds = streamingContext
      .textFileStream("my_stream_data")
      .window(Seconds(15)) // Window Duration 1

    val task5 = ds
      .map(_.split(";"))
      .map(line => (line(0) + "_" + line(2), (line(1), Integer.parseInt(line(3)))))
      .reduceByKey((x1, x2) => {if(x1._2 < x2._2) x2 else x1})
      .transform(_.sortBy(_._2._2, false))
      .print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
