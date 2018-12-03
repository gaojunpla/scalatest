//package sparksql
//
//import org.apache.spark.sql.SparkSession
//
//object Change {
//  def main(args: Array[String]): Unit = {
//    val session = SparkSession.builder()
//      .appName("Change")
//      .master("local[2]")
//      .getOrCreate()
//    import session.implicits._
//    val rdd= session.read.textFile(args(0)).flatMap(_.split(" ")).rdd
//
//    //RDD ---> DF
//
//    rdd.map((_,1))
//    .toDF("value","num")
//
//    //RDD ---> DSet
//    //import session.implicits._
//
//    val ds = rdd.map(colimn(_,1))
//      .toDS().show()
//
//    //DS ---> DF
//    //隐式转换
////    ds.toDF()
//
//    //DF ---> DS
//
//
//
//  }
//}
//case class colimn(str1: String,num:Int) extends Serializable