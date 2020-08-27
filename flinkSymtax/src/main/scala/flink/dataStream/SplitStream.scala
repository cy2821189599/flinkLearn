package flink.dataStream

import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}

/**
 * @author ：kenor
 * @date ：Created in 2020/8/25 22:42
 * @description：
 * @version: 1.0
 */
object SplitStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    // splitStream
    val splitStream = env.socketTextStream("localhost", 1111)
      .map(info => {
        val fields = info.split(",")
        val name = fields(0)
        val age = fields(1).toInt
        val height = fields(2).toInt
        val gender = fields(3)
        Person(name, age, height, gender)
      }).split((person) => {
      if (person.age >= 18) Seq("Adult") else Seq("young")
    })

    val adultStream: DataStream[(String, String)] = splitStream.select("Adult")
      .map(p => (p.name, p.gender))

    val youngStream = splitStream.select("young")
      .map(p => (p.name, p.gender, p.height))
    // connectStream
    val connectStream: ConnectedStreams[(String, String), (String, String, Int)] = adultStream.connect(youngStream)
    connectStream.map(
      adult => {
        (adult._1, adult._2)
      },
      young => {
        (young._1, young._2, young._3)
      }
    ).print()

    //unionStream
    val young = splitStream.select("young")
    val adult = splitStream.select("Adult")
    young.union(adult).print("union=>")

    env.execute()
  }

  case class Person(name: String, age: Int, height: Int, gender: String)

}
