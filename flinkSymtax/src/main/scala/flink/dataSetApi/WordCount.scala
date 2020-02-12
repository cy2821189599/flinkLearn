package flink.dataSetApi

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val filePath = "D:\\ReciveFile\\MiXing\\day18\\dataSource\\subjectaccess\\access.txt"
    val inputDateSet: DataSet[String] = env.readTextFile(filePath)
    import org.apache.flink.api.scala._
    val count = inputDateSet.flatMap(_.split("\t")).map((_, 1)).groupBy(0).sum(1)
    count.print()
  }

}
