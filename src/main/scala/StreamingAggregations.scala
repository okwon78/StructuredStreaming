import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object StreamingAggregations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Structured Streaming").master("local[3]").getOrCreate()
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
      .option("maxFilesPerTrigger", 2)
      .csv("/Users/amore/Dev/Spark/TalentOrigin/datasets/retail-data")

    val aggregateData = streamingData
      .where("Quantity > 10")
      .groupBy("InvoiceDate", "Country")
      .agg(sum("UnitPrice"))

    val query = aggregateData.writeStream
      .format("console")
      .queryName("aggregationTest")
      .outputMode(OutputMode.Complete())
      .start()

    query.awaitTermination()


  }
}
