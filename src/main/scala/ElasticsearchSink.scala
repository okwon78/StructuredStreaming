import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

object ElasticsearchSink {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Elasticsearch ETL")
      .master("local[3]")
      .getOrCreate()

    spark.conf.set("es.index.auto.create", "true")
    spark.conf.set("spark.sql.shuffle.partitions", 2)
    spark.conf.set("spark.default.parallelism", 2)
    spark.sparkContext.setLogLevel("INFO")

    val schema = new StructType()
      .add("Count", IntegerType)
      .add("Country", StringType)
      .add("Timestamp", TimestampType)

    val streamingDF = spark
      .readStream
      .schema(schema)
      .option("maxFilesPerTrigger", 1)
      .option("header", true)
      .csv("./data/source")

    val es_sink = streamingDF
      .writeStream
      //.format("es")
      .format("org.elasticsearch.spark.sql")
      .option("es.index.auto.create", "false")
      .option("checkpointLocation", "./checkpoint")
      .option("es.nodes.wan.only","true")
      .option("es.nodes.client.only", "false")
      .option("es.port", "9200")
      .option("es.nodes", "localhost")
      .outputMode("append")
      .start("index_test")

//    val console_sink = streamingDF
//      .writeStream
//      .format("console")
//      .option("truncate", "false")
//      .outputMode("append")
//      .start()

    es_sink.awaitTermination()
    //console_sink.awaitTermination()

  }
}
