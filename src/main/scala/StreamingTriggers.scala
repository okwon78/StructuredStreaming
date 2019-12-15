import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._

object StreamingTriggers {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Structured Streaming")
      .master("local[4]").getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", 2)
    spark.conf.set("spark.default.parallelism", 2)

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

    val query = streamingData.writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "./checkpoint")
      .queryName("StreamingTriggers")
      .outputMode(OutputMode.Append())
      .start().awaitTermination()
  }
}
