package sparksql

import org.apache.spark.sql.{SparkSession}

object wordCount {
  def main(args: Array[String]): Unit = {
    import org.apache.spark
    val session = SparkSession.builder()
      .appName("wordCount")
      .master("local")
      .getOrCreate()
    import session.implicits._
    val lines = session.read.textFile(args(0))
    val word =lines.flatMap(_.split(" "))
    word.groupBy($"value" as "word")
      .count()
      .sort($"count"desc)
      .show()

//    import org.apache.spark.sql.functions._
//    val w2 = lines.flatMap(_.split(" "))
//      .groupBy($"value")
//        .agg(count("*") as "count")
//        .orderBy($"count"desc)
//        .show()
//
    val word2 = lines.flatMap(_.split(" "))
    word2.createTempView("t_wc")
    val res = session.sql(
      """
        |SELECT value,count(*) counte
        |from t_wc
        |group by value
        |order by counte desc
        |limit 30
      """.stripMargin)
    res.show()

    session.stop()
  }
}
