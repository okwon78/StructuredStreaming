import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

object StructuredStreamingETL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Structured Streaming ETL").master("local[3]").getOrCreate()

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

    val filteredData = streamingData
      .where("Country = 'United Kingdom'")

    val query = filteredData.writeStream
      .format("console")
      .option("truncate", "false")
      .queryName("filteredByCountry")
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()
  }
}
