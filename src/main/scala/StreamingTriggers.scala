import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType, TimestampType}

object StreamingTriggers {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Structured Streaming").master("local[4]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val retailDataSchema = new StructType()
      .add("Idx", IntegerType)
      .add("InvoiceNo", StringType)
      .add("StockCode", StringType)
      .add("Description", StringType)
      .add("Quantity", IntegerType)
      .add("InvoiceDate", TimestampType)
      .add("UnitPrice", DoubleType)
      .add("CustomerId", StringType)
      .add("Country", StringType)
      .add("InvoiceTimestamp", TimestampType)

    val streamingData = spark
      .readStream
      .schema(retailDataSchema)
      .option("header", true)
      .option("maxFilesPerTrigger", 1)
      .csv("/Users/amore/Dev/Spark/TalentOrigin/datasets/retail-data")

    val tumblingWindowAggregations = streamingData
      .where("Country = 'United Kingdom'")
      .groupBy(
        window(col("InvoiceTimestamp"),"1 hours", "15 minutes"),
        col("Country")
      )
      .agg(sum("UnitPrice"))

    val sink = tumblingWindowAggregations
      .writeStream
      .trigger(Trigger.Once())
      //.trigger(Trigger.ProcessingTime("20 seconds"))
      .format("console")
      .option("truncate", "false")
      .outputMode("complete")
      .start()

    sink.awaitTermination()
  }
}
