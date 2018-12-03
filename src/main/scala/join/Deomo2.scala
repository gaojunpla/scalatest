//package join
//
//
//import org.apache.spark.sql.SparkSession
//object Deomo2 {
//  def main(args: Array[String]): Unit = {
//    val session = SparkSession.builder()
//      .master("local")
//      .appName("JoinDemo")
//      .getOrCreate()
//
//    session.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
//    session.conf.set("spark.sql.join.preferSortMergeJoin",true)
//    import session.implicits._
//    val df1 =Seq(
//      (0,"tonm"),(1,"jerr"),(2,"cat")
//    ).toDF("aid","name")
//
//    val df2 = Seq(
//      (0,18),(1,23),(2,34)
//    ).toDF("id","age")
//
//    df2.repartition(2)
//
//    val res = df1.join(df2,$"id" === $"aid")
//    res.show()
//    res.explain()
//  }
//}
