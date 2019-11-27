import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType, TimestampType}

object StreamingTriggers {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Structured Streaming").master("local[4]").getOrCreate()
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
      .groupBy(
        window(col("timestamp"),"1 hours", "15 minutes"),
        col("Country")
      )
      .agg(sum("Count"))

    val sink = tumblingWindowAggregations
      .writeStream
      //.trigger(Trigger.Once())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("console")
      .option("truncate", "false")
      .outputMode("update")
      .start()

    sink.awaitTermination()
  }
}
