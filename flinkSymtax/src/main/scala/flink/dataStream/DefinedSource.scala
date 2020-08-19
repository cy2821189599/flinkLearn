package flink.dataStream

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.io.Source

class DefinedSource(file:String) extends SourceFunction[LogInfo] {
  var cnt = 0
  var flag = true

  override def run(ctx: SourceFunction.SourceContext[LogInfo]): Unit = {
    val list = Source.fromFile(file).getLines().toList
    while (flag && cnt < list.length) {
      val e = list(cnt)
      val fields = e.split(",")
      val id = fields(0).toInt
      val time = fields(1).toInt
      val content = fields(2)
      val instance = new LogInfo(id, time, content)
      ctx.collect(instance)
      cnt += 1
    }
  }

  override def cancel(): Unit = {
    flag = false
  }
}