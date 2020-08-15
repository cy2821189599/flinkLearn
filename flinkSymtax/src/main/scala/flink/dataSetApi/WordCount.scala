package flink.dataSetApi

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem

object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
<<<<<<< HEAD
    //set the parallelism to 2
    val filePath = "hdfs://ns1/data/flink/input/2.txt"
    val inputDateSet: DataSet[String] = env.readTextFile(filePath) //read from  local fileSystem
    //    val inputDateSet: DataSet[String] = env.readTextFile("hdfs://ns1/data/flinkLearn/wordCount") //read from hdfs fileSystem
    import org.apache.flink.api.scala._
    val count = inputDateSet.flatMap(_.split("\\s")).setParallelism(2)
      .map((_, 1)).setParallelism(2)
      .groupBy(0)
      .sum(1).setParallelism(2)
    //    count.print()
    count.writeAsText("hdfs://ns1/data/flink/output/result.txt", FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1) //write to hdfs fileSystem,the default Parallelism is the number of cores
=======
    val filePath = "E:\\temp\\input\\2.txt"
    val inputDateSet: DataSet[String] = env.readTextFile(filePath) //read from  local fileSystem
    //    val inputDateSet: DataSet[String] = env.readTextFile("hdfs://ns1/data/flinkLearn/wordCount") //read from hdfs fileSystem
    import org.apache.flink.api.scala._
    val count = inputDateSet.flatMap(_.split("\\s")).map((_, 1)).groupBy(0).sum(1)
    //    count.print()
    count.writeAsText("hdfs://ns1/data/flinkLearn/wordCount", FileSystem.WriteMode.OVERWRITE)
    .setParallelism(1)//write to hdfs fileSystem,the default Parallelism is the number of cores
>>>>>>> beb86df230d24e63a874224e5792c9847876fdd2
    // if there is "sink", there will call env.execute() function
    env.execute(this.getClass.getSimpleName)
  }

}
