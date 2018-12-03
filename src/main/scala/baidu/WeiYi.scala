package baidu

object WeiYi {
  def main(args: Array[String]): Unit = {
    val x= Array(1,7,9)
    var num =0
    for(i<-0 until x.length){
      //二进制底层算法
      num = x(i) | num
      println(num)

    }
  }
}
