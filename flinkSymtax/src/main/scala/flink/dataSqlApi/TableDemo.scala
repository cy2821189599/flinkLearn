package flink.dataSqlApi

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.sinks.CsvTableSink
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
    //register tableSource,read csv file，convert to table
    //方法一
    val tableSource = CsvTableSource.builder()
      .path("D:\\ReciveFile\\项目4-store\\电商文档\\1.csv")
      .field("customer_id",Types.STRING)
      .build()
    //创建tableSource的方法二
    val csvTableSource = new CsvTableSource("D:\\ReciveFile\\项目4-store\\电商文档\\1.csv", Array("customer_id"), Array[TypeInformation[_]](Types
      .STRING))
    fbTableEnv.registerTableSource("csvTable",csvTableSource)
    val table = fbTableEnv.sqlQuery("select * from csvTable")
//      .toDataSet[Row]
//      .print()
    //tableSink
    val tableSink = new CsvTableSink("C:\\Users\\cy282\\Desktop\\test.csv","|",1,WriteMode.OVERWRITE)
    fbTableEnv.registerTableSink("csvTableSink",Array("customer_id"),Array[TypeInformation[_]](Types.STRING),tableSink)
    table.insertInto("csvTableSink")
    table.toDataSet[Row].print()
  }
  case class WC(word: String, frequency: Long)
}
