import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/**
 * Made by following
 * https://github.com/phatak-dev/spark2.0-examples
 * http://blog.madhukaraphatak.com/introduction-to-spark-structured-streaming-part-6/
 */

object StreamJoin {

  case class Sales(
                    transactionId: String,
                    customerId:    String,
                    itemId:        String,
                    amountPaid:    Double)
  case class Customer(customerId: String, customerName: String)
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("spark://128.195.52.129:7077")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.executor.cores", "1")
      .config("spark.executor.memory", "5G")
      .appName("stream-static join example")
      .getOrCreate()

    //create stream from socket
    val socketStreamDf = sparkSession.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 50050)
      .load()

    import sparkSession.implicits._

    // val url = this.getClass.getClassLoader.getResource("customers.csv")
    //take customer data as static df
    val customerDs = sparkSession.read
      .format("csv")
      .option("header", true)
      .load("/home/avinash/Downloads/Stream-Static-Join/src/main/resources/customers.csv")
      .as[Customer]

    import sparkSession.implicits._
    val dataDf = socketStreamDf.as[String].flatMap(value ⇒ value.split(" "))
    val salesDs = dataDf
      .as[String]
      .map(value ⇒ {
        val values = value.split(",")
        Sales(values(0), values(1), values(2), values(3).toDouble)
      })

    val joinedDs = salesDs
      .join(customerDs.hint("shuffle_hash"), "customerId")
    //create sales schema
    val query =
      joinedDs.writeStream.format("console").outputMode(OutputMode.Append())

    query.start().awaitTermination()
  }
}
