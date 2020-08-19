package flink.dataStream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
 * @author ：kenor
 * @date ：Created in 2020/8/19 22:06
 * @description：flink user defined source
 * @version: 1.0
 */
object UDFSource extends App {
  private val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  import org.apache.flink.api.scala._

  env.addSource(new DefinedSource("E:\\temp\\input\\4.txt")).print()
  env.execute(this.getClass.getSimpleName)

}
