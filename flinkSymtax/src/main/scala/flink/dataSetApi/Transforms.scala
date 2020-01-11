package flink.dataSetApi

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.util.Collector


object Transforms {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala.createTypeInformation

    val environment = ExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(3)
    //map算子
    val intPair = environment.fromCollection(Array((1, 2), (2, 3), (4, 5)))
    intPair.map(pair => {
      pair._1 + pair._2
    }).print()
    val strPair = environment.fromCollection(Array(("tom", 18), ("lisi", 19), ("zhangsan", 18)))
    //groupBy算子，跟spark groupBy算子有所不同
    val res = strPair.groupBy(0).reduce { (x, y) => {
      (x._1, x._2 + y._2)
    }
    }
    res.print()
    //多个分组条件
    val dsA = environment.fromCollection(Array(A("zhangsan", "shenzhen", 18), A("lisi", "shenzhen", 19), A("wangwu",
      "beijing", 20), A("zhangsan", "shenzhen", 20)))
    dsA.groupBy("name", "gender")
      .max("age")
      .print()
    dsA.groupBy(0, 1)
      .reduce((a, b) => A(a.name, a.gender, a.age + b.age))
      .print()
    println()
    // 分组排序
    val output = dsA.groupBy(0, 1).
      sortGroup(2, Order.ASCENDING)
      .reduceGroup {
        (in, out: Collector[A]) =>
          var prev: A = null
          for (t <- in) {
            if (prev == null || prev != t)
              out.collect(t)
            prev = t
          }
      }
    output.print()
    //分组内聚合
    val outputCombine = dsA.groupBy(0, 1)
      .combineGroup((in, out: Collector[(String, String, Int)]) => {
        var name: String = null
        var gender: String = null
        var count: Int = 0
        for (t <- in) {
          name = t.name
          gender = t.gender
          count = count + 1
        }
        out.collect((name, gender, count))
      })
    outputCombine.print()
    // 分组内聚合
    val dsB = environment.fromCollection(Array((1, "apple", 6.8), (2, "apple", 7.2), (3, "pair", 5.8), (5, "pair", 5.6), (5,
      "banana", 3.5), (5, "banana", 3.5)))
    val aggDS = dsB.groupBy(1).aggregate(Aggregations.SUM, 0).and(Aggregations.MIN, 2)
    aggDS.print()
    // minBy、maxBy,从前往后选择
    dsB.groupBy(1).minBy(0, 2).print()
    //去重,取第一个
    dsB.distinct().print()
    println()
    dsB.distinct(1).print()
    //join 形成一个元组，第一个元素是左边的元组，第二个元组是右边的元素
    val dsC = environment.fromCollection(Array(("apple", true), ("banana", false), ("pair", true), ("orange", false)))
    dsB.join(dsC).where(1).equalTo(0).print()
    println()
    //提示：hint，提示join的第二张表很大,从而内部会优化连接策略
    dsB.joinWithHuge(dsC).where(1).equalTo(0).print()
    // hint：提示join的第二张表很小
    dsB.joinWithTiny(dsC).where(1).equalTo(0).print()
    //还可以join时选择连接提示算法
    //外连接
    val outputOutJoin = dsB.rightOuterJoin(dsC).where(1).equalTo(0) {
      (a, b) => {
        val fruit = if (a == null) "other" else a._2
        (fruit, b._1)
      }
    }
    outputOutJoin.print()
    // cross 笛卡尔积
    //coGroup 隐式函数处理时只取左边的每组元素的第一个元素和右边的每组的每个元素的数据去计算
    val dsD = environment.fromCollection(Array(("apple", 0.8), ("pair", 0.9), ("banana", 0.8), ("orange", 0.5), ("apple", 0.7)
      , ("pair", 0.8), ("banana", 0.9), ("orange", 0.7), ("orange", 0.7), ("orange", 0.7), ("orange", 0.7), ("orange", 0.7),
      ("orange", 0.7), ("orange", 0.7), ("orange", 0.7)))
    val coG = dsB.coGroup(dsD).where(1).equalTo(0) {
      (dsB, dsD, out: Collector[Double]) => {
        //          println(dsB.toBuffer)
        //          println(dsD.toBuffer)
        for (b <- dsB) {
          for (d <- dsD) {
            println(b + "\t" + d)
            out.collect(b._3 * d._2)
          }
        }
      }
    }
    coG.print()
    //union
    // Rebalance 消除数据倾斜,很方便的算子
    dsD.mapPartition(itr=>{
      itr.foreach(println)
      itr.toList
    }).collect()
    println("---------------------")
    dsD.rebalance().mapPartition(itr=>{
      itr.foreach(println)
      itr.toList
    }).collect()

    //取前n条数据
    dsA.first(2).print()

  }

  case class A(name: String, gender: String, age: Int)

}
