import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object ThumblingWindow {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Structured Streaming").master("local[3]").getOrCreate()

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
        .option("header", true)
        .option("maxFilesPerTrigger", 1)
        .csv("./data/source")

    val tumblingWindowAggregations = streamingData
        .groupBy(
          window(col("timestamp"),"1 hours"),
          col("Country")
        )
        .agg(sum("Count"))
        .orderBy("window")

    val sink = tumblingWindowAggregations
        .writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode("complete")
        .start()

    sink.awaitTermination()
  }
}
