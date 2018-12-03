package UDF

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class udaf extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = {
    //输入数据类型
    StructType(Array(StructField("str",StringType,true)))
  }

  override def bufferSchema: StructType = {
    //中间转换结果类型
    StructType(Array(StructField("count",IntegerType,true)))
  }
  //返回结果类型
  override def dataType: DataType = {
    IntegerType
  }

  //数据一致性
  override def deterministic: Boolean = true

  //初始化，局部聚合使用
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  //更新结果
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) =buffer.getAs[Int](0)+1
  }

  //聚合函数做全局函数
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0)+buffer2.getAs[Int](0)
  }

  //返回聚合结果
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }
}
