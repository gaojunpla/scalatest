package UDF

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object uff2 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("uff2")
      .master("local")
      .getOrCreate()

    val rdd0 = session.sparkContext.textFile(args(0)).flatMap(_.split(" ").map(Row(_)))

    val structType = StructType(Array(StructField("name",StringType)))
    val x1 = session.createDataFrame(rdd0,structType)
    x1.createTempView("oldman")

    session.udf.register("myudaf",new udaf)
    session.sql("select name,myudaf(name) as count from oldman group by name order by count desc limit 30").show()
  }
}
