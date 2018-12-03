package Review

import org.apache.spark.sql.SparkSession

object readJson {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local[*]")
      .appName("readJson")
      .getOrCreate()

    val csvFile = session.read.textFile("D:\\atmp\\input\\5000")
    import session.implicits._
    val data = csvFile.map(file =>{
      val str = file.split(",")
      val name = str(0)
      val idkind = str(3)
      val id = str(4)
      val gender = str(5)
      val adress = str(7)
      (name,idkind,id,gender,adress)
    })
    val df = data.toDF("name","idkind","id","gender","adress")


    df.show()

  }
}
