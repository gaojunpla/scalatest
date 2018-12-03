//package baidu
//
//import org.apache.spark.sql.SparkSession
//
//object Find {
//  def main(args: Array[String]): Unit = {
//    val session = SparkSession.builder()
//      .appName("Find")
//      .master("local[*]")
//      .getOrCreate()
//    val lines = session.sparkContext.textFile(args(0))
//
//
//
//    import session.implicits._
//    val rdd0 = lines.map(line =>{
//
//      val dt = line.split("\t")
//      val time =dt(0)
//      val id = dt(1)
//      val find =dt(2)
//      val num =dt(3)
//      val site=dt(4)
//      (time,id,find,num,site)
//    }).toDF("time","id","find","num","site")
//    rdd0.createTempView("t_log")
//
//
//
//  }
//}
