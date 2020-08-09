package flink.dataSetApi

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem

object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val filePath = "E:\\ReciveFile\\MiXing\\day18\\dataSource\\subjectaccess\\access.txt"
    val inputDateSet: DataSet[String] = env.readTextFile(filePath)
    import org.apache.flink.api.scala._
    val count = inputDateSet.flatMap(_.split("\\s")).map((_, 1)).groupBy(0).sum(1)
//    count.print()
    count.writeAsText("hdfs://ns1/data/flinkLearn/wordCount",FileSystem.WriteMode.OVERWRITE)
    env.execute(this.getClass.getSimpleName)
  }

}
