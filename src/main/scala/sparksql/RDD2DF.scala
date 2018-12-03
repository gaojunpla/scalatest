package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object RDD2DF {
  def main(args: Array[String]): Unit = {
    //2.x 执行入口

    val session =SparkSession.builder()
      .appName("").master("").getOrCreate()
    //创建RDD
    val lines = session.sparkContext.textFile(args(0))
    //数据整理
    val rowRDD = lines.map(line =>{
      val f = line.split(" ")
      val id = f(0).toInt
    })

    //定义schema
    val schema = StructType(List(
      StructField("id",LongType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true),
      StructField("value",DoubleType,true)
    ))

    val df = session.createDataFrame(schema)

  }
}
