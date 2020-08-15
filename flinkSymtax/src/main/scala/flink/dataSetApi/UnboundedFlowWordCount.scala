package flink.dataSetApi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * use unboundedFLow API to write boundedFLow program
 * the unboundedFLow API is combined with boundedFlow API
 */
object UnboundedFlowWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // set the parallelism to 2
    env.setParallelism(2)
    val txt = env.readTextFile("e://temp/input/1.txt")
    import org.apache.flink.api.scala._
    val keys = txt.flatMap(_.split("\\s"))
      .map(Word(_, 1)).setParallelism(1)
      .keyBy("key")
      .reduce((a, b) => Word(a.key, a.num + b.num)) //or use sum(2)
      .rebalance

    keys.print()
    /*  the result(treat the boundedFlow as unboundedFlow)
      2> Word(boundedFLow,1)
      2> Word(unboundedFLow,1)
      2> Word(to,1)
      2> Word(program,1)
      1> Word(use,1)
      1> Word(API,1)
      1> Word(write,1)
      1> Word(the,1)
      1> Word(API,2)
      1> Word(combined,1)
      1> Word(boundedFlow,1)
      2> Word(unboundedFLow,2)
      2> Word(is,1)
      2> Word(with,1)
      2> Word(API,3)
     */

    env.execute("UnboundedFlowWordCount")

  }

  case class Word(key: String, num: Int)

}
