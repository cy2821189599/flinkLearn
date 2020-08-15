package flink.dataStream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object SocketWindowWordCount {

  def main(args: Array[String]): Unit = {

    // the port to connect to
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        println("No port specified. Please run 'SocketWindowWordCount --host <host> --port <port>'")
        return -1
      }
    }
    val host: String = try {
      ParameterTool.fromArgs(args).get("host")
    } catch {
      case e: Exception => {
        println("No port specified. Please run 'SocketWindowWordCount --host <host> --port <port>'")
        return -1
      }
    }

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text = env.socketTextStream(host, port, '\n')

    import org.apache.flink.api.scala._
    // parse the data, group it, window it, and aggregate the counts
    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")  //相比spark比较特殊常用的算子
      .timeWindow(Time.seconds(10), Time.seconds(10))
      .sum("count")

    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }

  // Data type for words with count
  case class WordWithCount(word: String, count: Long)

}
