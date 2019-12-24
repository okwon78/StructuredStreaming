import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.log4j._


object KafkaSource {

  val servers = Array("b-1.apne2apdataabcbdp.3brgpi.c4.kafka.ap-northeast-2.amazonaws.com:9094",
                      "b-2.apne2apdataabcbdp.3brgpi.c4.kafka.ap-northeast-2.amazonaws.com:9094",
                      "b-3.apne2apdataabcbdp.3brgpi.c4.kafka.ap-northeast-2.amazonaws.com:9094")

  def main(args: Array[String]): Unit = {

    println("## Structured Streaming with Kafka ##")
    val topic: String = "bdp_apne2_prd_rl_on_ari"
    val bootstrap_servers: String = servers.mkString(",")
    println("## Structured Streaming with Kafka ##")

    println("topic: " + topic)
    println("bootstrap_servers: " + bootstrap_servers)

    val spark = SparkSession.builder().appName("Spark Streaming Aggregations").master("yarn").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val logger: Logger = Logger.getLogger("org.apache.spark")
    logger.info("bootstrap_servers " + bootstrap_servers)

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap_servers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .option("maxOffsetsPerTrigger", 10)
      .load()

    //val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    df.writeStream
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
