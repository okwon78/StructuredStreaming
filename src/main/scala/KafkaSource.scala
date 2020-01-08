import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.log4j._


object KafkaSource {

  val servers = Array("b-1.apne2apdataabcbdp.3brgpi.c4.kafka.ap-northeast-2.amazonaws.com:9092",
                      "b-2.apne2apdataabcbdp.3brgpi.c4.kafka.ap-northeast-2.amazonaws.com:9092",
                      "b-3.apne2apdataabcbdp.3brgpi.c4.kafka.ap-northeast-2.amazonaws.com:9092")

  val topic: String = "bdp_apne2_prd_rl_on_ari"


  def main(args: Array[String]): Unit = {

    val bootstrap_servers: String = servers.mkString(",")

    val spark = SparkSession.builder()
      .appName("Spark Streaming Aggregations")
      .config("spark.sql.streaming.schemaInference", true)
      .config("spark.dynamicAllocation.enabled", true)
      .config("spark.dynamicAllocation.executorIdleTimeout", "360s")
      .config("spark.dynamicAllocation.minExecutors", "2")
      .config("spark.default.parallelism", "2")
      .master("local[2]").getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    val logger: Logger = Logger.getLogger("org.apache.spark")
    logger.info("#####################################")
    logger.info("## Structured Streaming with Kafka ##")
    logger.info("topic: " + topic)
    logger.info("bootstrap_servers: " + bootstrap_servers)
    logger.info("## Structured Streaming with Kafka ##")
    logger.info("#####################################")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap_servers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("kafkaConsumer.pollTimeoutMs", 512)
      .option("fetchOffset.numRetries", 1000)
      .option("fetchOffset.retryIntervalMs", 100000)
      .option("maxOffsetsPerTrigger", 10)
      .load()

    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    query.writeStream
      .format("console")
      //.option("truncate", "false")
      //.queryName("kafka")
      .outputMode(OutputMode.Append())
      .start().awaitTermination()

//    query.writeStream
//      .format("csv")
//      .option("header", "false")
//      .option("path", "/home/hadoop/output")
//      .option("checkpointLocation", "/home/hadoop/checkpoint")
//      .outputMode(OutputMode.Append())
//      .start().awaitTermination()

  }
}
