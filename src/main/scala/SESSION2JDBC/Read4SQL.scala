package SESSION2JDBC
import org.apache.spark.sql.SparkSession

object Read4SQL {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("Read4SQL")
      .master("local")
      .getOrCreate()

    val res = session.read.format("jdbc").options(
      Map("url"->"jdbc:mysql://192.168.10.130:3306/test",
      "driver"->"com.mysql.jdbc.Driver",
      "dbtable"->"myo",
      "user"->"root",
      "password"->"root"
      )
    ).load().distinct()


    //write
//    val props = new Properties()
//    props.put("user","root")
//    props.put("password","root")
//    props.put("driver","com.mysql.jdbc.Driver")
//    res.write.mode("append").jdbc("jdbc:mysql://192.168.10.130:3306/test","myo",props)
    res.printSchema()
    res.show()
  }
}
