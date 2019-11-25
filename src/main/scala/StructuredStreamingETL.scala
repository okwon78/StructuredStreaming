import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

object StructuredStreamingETL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Structured Streaming").master("local[3]").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country,InvoiceTimestamp
    val retailDataSchema = new StructType()
      .add("Idx", IntegerType)
      .add("InvoiceNo", StringType)
      .add("StockCode", StringType)
      .add("Description", StringType)
      .add("Quantity", IntegerType)
      .add("InvoiceDate", TimestampType)
      .add("UnitPrice", DoubleType)
      .add("CustomerId", IntegerType)
      .add("Country", StringType)
      .add("InvoiceTimestamp", TimestampType)

    val streamingData = spark
      .readStream
      .schema(retailDataSchema)
      .option("header", true)
      .csv("/Users/amore/Dev/Spark/TalentOrigin/datasets/tmp")

    val filteredData = streamingData.filter("Country = 'United Kingdom'")

    val query = filteredData.writeStream
      .format("console")
      .queryName("filteredByCountry")
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()
  }
}
