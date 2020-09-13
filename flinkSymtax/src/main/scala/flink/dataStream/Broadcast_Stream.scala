package flink.dataStream

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * @author ：kenor
 * @date ：Created in 2020/9/13 16:28
 * @description：
 * @version: 1.0
 */
object Broadcast_Stream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val ds1: DataStream[(Int, Char)] = env.fromElements((1, 'f'), (2, 'm'))

    val ds2 = env.socketTextStream("localhost", 9999)
      .filter(_.nonEmpty)
      .map(s => {
        val fields = s.split(",")
        val id = fields(0).trim.toInt
        val name = fields(1).trim
        val genderFlag = fields(2).trim.toInt
        val address = fields(3).trim
        (id, name, genderFlag, address)
      })

    val broadcastStateDesc: MapStateDescriptor[Integer, Character] = new MapStateDescriptor("genderInfo",
      BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.CHAR_TYPE_INFO)
    // broadcast var
    val bcStream: BroadcastStream[(Int, Char)] = ds1.broadcast(broadcastStateDesc)

    val bcConnectStream: BroadcastConnectedStream[(Int, String, Int, String), (Int, Char)] = ds2.connect(bcStream)

    // process broadCastStream
    bcConnectStream.process[(Int, String, Char, String)](new MyBroadcastProcessFunction(broadcastStateDesc))
      .print()

    env.execute(this.getClass.getSimpleName)
  }

  class MyBroadcastProcessFunction(broadcastStateDesc: MapStateDescriptor[Integer, Character])
    extends BroadcastProcessFunction[(Int, String, Int, String), (Int, Char), (Int, String, Char, String)] {

    // process every element of the dataStream
    /**
     *
     * @param value
     * @param ctx read from broadcast of the ctx
     * @param out collect the processed result
     */
    override def processElement(value: (Int, String, Int, String),
                                ctx: BroadcastProcessFunction[(Int, String, Int, String), (Int, Char), (Int, String, Char, String)]#ReadOnlyContext,
                                out: Collector[(Int, String, Char, String)]): Unit = {
      val gender = ctx.getBroadcastState(broadcastStateDesc).get(value._3)
      out.collect(value._1, value._2, gender, value._4)
    }

    // process every element of the broadcast var
    /**
     *
     * @param value
     * @param ctx set to broadcast of the ctx
     * @param out
     */
    override def processBroadcastElement(value: (Int, Char),
                                         ctx: BroadcastProcessFunction[(Int, String, Int, String), (Int, Char), (Int, String, Char, String)]#Context,
                                         out: Collector[(Int, String, Char, String)]): Unit = {
      ctx.getBroadcastState(broadcastStateDesc).put(value._1, value._2)
    }
  }

}

