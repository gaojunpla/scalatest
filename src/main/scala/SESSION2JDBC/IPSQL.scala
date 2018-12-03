//package SESSION2JDBC
//
//import org.apache.spark.sql.SparkSession
//
//
//object IPSQL {
//  //通过字典数据和日志信息分别放到dataframe，放到两张表里
//  def main(args: Array[String]): Unit = {
//
//    val sparkSession=SparkSession.builder()
//      .appName("ip2").master("local[*]").getOrCreate()
//
//    //处理字典数据
//    val dictds=sparkSession.read.textFile(args(0))
//    import sparkSession.implicits._
//    val dictdf= dictds.map(line=>{
//      val fields=line.split("\\|")
//      val start=fields(2).toLong
//      val stop=fields(3).toLong
//      val province=fields(7)
//      (start,stop,province)
//    }).toDF("start","stop","province")
//
//    val logds=sparkSession.read.textFile(args(1))
////    import sparkSession.implicits._
//    val logdf= logds.map(line=>{
//      val fields=line.split("\\|")
//      val ip=fields(1)
//      val ipL=utils.ipLong(ip)
//      ipL
//
//    }).toDF("ip_num")
//    dictdf.createTempView("v_dicts")
//    logdf.createTempView("v_logs")
//
//    val res=sparkSession.sql("select province,count(*) counts from v_dicts  join v_logs " +
//
//      "on(ip_num>=start and ip_num<=stop) group by province order by counts desc")
//
//    res.show()
//
//    sparkSession.stop()
//
//  }
//}
//
//object utils{
//  def ipLong(ip:String)={
//    val f=ip.split("[.]")
//    var ipNum=0L
//    for(i<- 0 until f.length)
//    {
//      ipNum=f(i).toLong | ipNum << 8L
//    }
//    ipNum
//  }
//
//  def binarySearch(arr:Array[(String,String,String)],ip:Long):Int={
//    var low=0
//    var high=arr.length
//
//    while(low<=high){
//      val middle=(low+high)/2
//      if((ip>=arr(middle)._1.toLong) && (ip<=arr(middle)._2.toLong))
//      {
//        return middle
//      }
//
//      if(ip<arr(middle)._1.toLong){
//        high=middle-1
//      }else{
//        low=middle+1
//      }
//    }
//    -1
//  }
//}
