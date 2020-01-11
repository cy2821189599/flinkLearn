package flink.dataSqlApi

import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

object TableDemo {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    import org.apache.flink.table.api.scala._

    val fbEnv = ExecutionEnvironment.getExecutionEnvironment
    val fbTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(fbEnv)
    // DataSet->table->DataSet
    val input: DataSet[WC] = fbEnv.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
    val expr: Table = input.toTable(fbTableEnv)
    val result = expr
      .groupBy('word)
      .select('word, 'frequency.sum as 'frequency)
      .filter('frequency === 2)
      .toDataSet[WC]
    result.print()
    //register table
    fbTableEnv.registerTable("table1",expr)
    fbTableEnv.sqlQuery("select * from table1").toDataSet[Row].print()
    fbTableEnv.scan("table1").select("word").toDataSet[Row].print()
    //register tableSource
    CsvTableSource.builder().path("D:\\ReciveFile\\项目4-store\\电商文档\\1.csv").field("ss",Types.BOOLEAN)
    new CsvTableSource("D:\\ReciveFile\\项目4-store\\电商文档\\1.csv",Array("name"),Array(Types.STRING))
  }
  case class WC(word: String, frequency: Long)
}
