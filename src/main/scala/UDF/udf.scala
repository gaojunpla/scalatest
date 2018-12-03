package UDF

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object udf {
  def ma(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("udf")
      .master("local")
      .getOrCreate()
    val rdd0 =session.sparkContext.parallelize(
      Array("Tom","Jerry","hook","Tom","hook","hook").map(Row(_))
    )
//    rdd0.foreach(println)
//------------------分割线--------------------------
    val structType = StructType(Array(StructField("name",StringType,true)))
    val namedf =session.createDataFrame(rdd0,structType)

    //注册表
    namedf.createTempView("v_name")

    //注册自定义函数
    session.udf.register("myudaf",new udaf)
    session.sql(
      """
        |select name,myudaf(name)
        |from v_name
        |group by name
      """.stripMargin).show()


    //--------------------分割线-------------------------------
    val fun =(str:String )=>{"gao"+str}
    //注册
//    session.udf.register("fun",fun)
//    //使用自定义函数
    val res = session.sql(
      """
        |select name,fun(name) as uu
        |from v_name
      """.stripMargin).show()


  }
}
class udf{}