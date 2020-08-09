package flink.dataSetApi

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val filePath = "E:\\ReciveFile\\MiXing\\day18\\dataSource\\subjectaccess\\access.txt"
    val inputDateSet: DataSet[String] = env.readTextFile(filePath)
    import org.apache.flink.api.scala._
    val count = inputDateSet.flatMap(_.split("\\s")).map((_, 1)).groupBy(0).sum(1)
//    count.print()
    count.writeAsText("hdfs://ns1/data/flinkLearn/wordCount")
    env.execute(this.getClass.getSimpleName)
  }

}
