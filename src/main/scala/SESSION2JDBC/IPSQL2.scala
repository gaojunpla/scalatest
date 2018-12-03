//package SESSION2JDBC
//
//import org.apache.spark.sql.SparkSession
//
//object IPSQL2 {
//  def main(args: Array[String]): Unit = {
//    val sparkSession=SparkSession.builder()
//      .appName("ip3").master("local[*]").getOrCreate()
//
//
//    //处理字典数据
//    val dictds=sparkSession.read.textFile("D:\\spark\\sparksqlDemo\\src\\main\\sparksql\\ip.txt")
//    import sparkSession.implicits._
//    val tdictds= dictds.map(line=>{
//      val fields=line.split("\\|")
//      val start=fields(2)
//      val stop=fields(3)
//      val province=fields(6)
//      (start,stop,province)
//    })
//
//
//    //把字典数据在dirver端收集
//    val t=tdictds.collect()
//    //广播到worker端
//    val broadcast=sparkSession.sparkContext.broadcast(t)
//    //读取访问日志
//    val loginfo=sparkSession.read.textFile("D:\\spark\\sparksqlDemo\\src\\main\\sparksql\\http.log")
//
//    //数据整理
//    val logdf=loginfo.map(line=>{
//      val fields=line.split("\\|")
//
//      val ip=fields(1) //ip信息
//
//      val ipnum=utils.ipLong(ip)
//
//      ipnum
//    }).toDF("ip_num")
//
//    logdf.createTempView("v_log")
//
//    //自定义函数把ipnum跟广播变量中的值分别比较,把Ip地址映射成省份
//    val iptoprovince= (ipnum:Long)=>{
//
//      //读取字典
//      val ipdict=broadcast.value
//
//      val index=utils.binarySearch(ipdict,ipnum)
//
//      var province ="未知"
//      if(index != -1){
//
//        province=ipdict(index)._3
//      }
//      province
//    }
//
//    //注册自定义的函数
//    sparkSession.udf.register("ip2province",iptoprovince)
//    val res=sparkSession.sql("select ip2province(ip_num) province,count(*) as counts "+
//      "from v_log group by province order by counts ").show()
//
//    sparkSession.stop()
//
//  }
//
//}
//
//
