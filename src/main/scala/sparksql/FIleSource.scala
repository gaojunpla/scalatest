package sparksql

import org.apache.spark.sql.SparkSession

object FIleSource {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("FIleSource")
      .master("local")
      .getOrCreate()

    val df = session.read.json(args(0))

    val df2 = session.read.format("json").load(args(0))
    import  session.implicits._
    val res = df.where($"age" < 30)
    res.printSchema()
    res.show()

  }
}
