import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object StreamingAggregations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Streaming Aggregations").master("local[3]").getOrCreate()

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

    val aggregateData = streamingData
      .groupBy("Country")
      .agg(sum("Count"))

    val query = aggregateData.writeStream
      .format("console")
      .option("truncate", "false")
      .queryName("aggregationTest")
      .outputMode(OutputMode.Complete())
      .start()

    query.awaitTermination()


  }
}
