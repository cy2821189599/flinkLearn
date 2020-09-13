package flink.dataStream

import flink.dataStream.KafkaSink.Person
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author ：kenor
 * @date ：Created in 2020/9/13 21:12
 * @description：
 * @version: 1.0
 */
object Accumulator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    env.socketTextStream("localhost",9999)
      .filter(_.nonEmpty)
      .map(info=>{
        val fields = info.split(",")
        val name = fields(0).trim
        val age = fields(1).trim.toInt
        val height = fields(2).trim.toInt
        val gender = fields(3).trim
        Person(name, age, height, gender)
      })
      .map(new MyRichMapFunction)
      .print()

    val result = env.execute()
    val totalCnt = result.getAccumulatorResult[Int]("totalAcc")
    val adultCnt = result.getAccumulatorResult[Int]("adultAcc")
    val youngCnt = result.getAccumulatorResult[Int]("youngAcc")
    println("total:",totalCnt)
    println("adult:",adultCnt)
    println("young:",youngCnt)
  }

  class MyRichMapFunction extends RichMapFunction[Person ,(Person,String)]{

    private var totalAcc:IntCounter = _
    private var adultAcc:IntCounter = _
    private var youngAcc:IntCounter = _

    override def open(parameters: Configuration): Unit = {
      totalAcc = new IntCounter(0)
      adultAcc = new IntCounter(0)
      youngAcc = new IntCounter(0)
      //register counter
      var context = getRuntimeContext
      context.addAccumulator("totalAcc",totalAcc)
      context.addAccumulator("adultAcc",adultAcc)
      context.addAccumulator("youngAcc",youngAcc)
    }

    override def map(in: Person): (Person, String) = {
      totalAcc.add(1)
      if(in.age>17){
        adultAcc.add(1)
        (in,"pass")
      }else{
        youngAcc.add(1)
        (in,"Intercept")
      }
    }

    override def close(): Unit = {
        super.close()
    }
  }

}
