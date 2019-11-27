import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum, window}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._


object StreamingWithWatermark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("StreamingWithWatermark").master("local[3]").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = new StructType()
      .add("Count", IntegerType)
      .add("Country", StringType)
      .add("timestamp", TimestampType)

    val streamingData = spark
      .readStream
      .schema(schema)
      .option("maxFilesPerTrigger", 1)
      .option("header", true)
      .csv("./data/source")

    val tumblingWindowAggregations = streamingData
      .withWatermark("timestamp", "1 minutes")
      .groupBy(
        window(col("timestamp"),"1 hours"),
        col("Country")
      )
      .agg(sum("Count"))

    val sink = tumblingWindowAggregations
      .writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("update")
      .start()

    sink.awaitTermination()
  }
}
