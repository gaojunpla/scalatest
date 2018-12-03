package Struct_Streamming


import org.apache.spark.sql.SparkSession

object StreammingDemo {
  def main(args: Array[String]): Unit = {
    //
    val session = SparkSession.builder()
      .appName("haha")
      .master("local[*]")
      .getOrCreate()

    val lines = session.readStream.format("socket")
      .option("host","master")
      .option("port","6666")
      .load()

    import session.implicits._
    val line = lines.as[String]
    val res = line.flatMap(_.split(" ")).groupBy("value").count()
    val res2 = res.writeStream.outputMode("complete").format("console").start()

    res2.awaitTermination()

  }
}
