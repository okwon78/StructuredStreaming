import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object StreamingTriggers {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Structured Streaming")
      .config("es.nodes", "localhost")
      .config("es.port", "9200")
      //.config("es.index.auto.create", "true")
      .config("es.nodes.wan.only", "true")
      .config("es.net.http.auth.user", "elastic")
      .config("es.net.http.auth.pass", "changeme")
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

    val sink = streamingData
      .writeStream
      .outputMode("append")
      .format("org.elasticsearch.spark.sql")
      .option("es.resource", "index_test/doc")
      .option("es.nodes", "localhost")
      .option("checkpointLocation", "./checkpoint")
      .start().awaitTermination()
  }
}
