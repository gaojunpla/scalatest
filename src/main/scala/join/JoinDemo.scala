//package join
//
//import org.apache.spark.sql.SparkSession
//
//object JoinDemo {
//  def main(args: Array[String]): Unit = {
//    val session = SparkSession.builder()
//      .master("local")
//      .appName("JoinDemo")
//      .getOrCreate()
//
//    import session.implicits._
//    val lines = session.createDataset(
//      Array("1,lillei,cn","2,tom,usa","3,jerry,uk"))
//
//    val tups = lines.map(line =>{
//      val x =line.split(",")
//      val id = x(0).toInt
//      val name =x(1)
//      val con =x(2)
//
//      (id,name,con)
//    })
//
//    val df1 =tups.toDF("id","name","con")
//
//    val contry = session.createDataset(
//      List("cn,中国","usa,美国","uk,英国")
//    )
////    val tup2 = contry.map(con1 =>{
////      val x2 =con1.split(",")
////      val ename =x2(0)
////      val cname =x2(1)
////      (ename,cname)
////    })
////    val df2 =tup2.toDF("ename","cname")
//    创建视图
////    df1.createTempView("t_user")
////    df2.createTempView("t_con")
////
////    val res = session.sql(
////      """
////        |select u.id,u.name,c.cname
////        |from t_user u join t_con c on u.con=c.ename
////      """.stripMargin)
////
////    res.show()
////
////    res.explain()
////    session.stop()
////
////  }
////}
////