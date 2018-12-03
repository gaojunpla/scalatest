package Review

import org.apache.spark.sql.SparkSession

object IPFind {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local[*]")
      .appName("coco")
      .getOrCreate()

    val http = session.read.textFile("D:\\atmp\\input\\http.log")
    import session.implicits._
    val httpmsg = http.map(x =>{
      val ips = x.split("[|]")(1).split("[.]")
      val ip2 = ips(0).toInt*255*255*255+ips(1).toInt*255*255+ips(2).toInt*255+ips(3).toInt
      (ip2)
    })

    val df1 = httpmsg.toDF("IP1")
    df1.createTempView("df1")
    val IP = session.read.textFile("D:\\atmp\\input\\ip.txt")
    val ipmsg = IP.map(y=>{
      val m1 = y.split("[|]")
      val start = m1(2)
      val end = m1(3)
      val province = m1(6)
      val city = m1(7)
      (start,end,province,city)
    })
    val df2 = ipmsg.toDF("start","end","province","city")
    df2.createTempView("df2")
    session.sql(
      """
        |select df2.province,count(1) as count
        |from df1 join df2
        |where IP1>df2.start and IP1<df2.end
        |group by df2.province
        |order by count desc
      """.stripMargin).show()
    session.stop()

  }
}
