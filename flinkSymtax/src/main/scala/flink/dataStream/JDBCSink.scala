package flink.dataStream

import java.sql.{Connection, DriverManager, PreparedStatement}

import flink.dataStream.KafkaSink.Person
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author ：kenor
 * @date ：Created in 2020/9/9 23:02
 * @description：
 * @version: 1.0
 */
object JDBCSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val result = env.socketTextStream("localhost", 8888)
      .filter(_.nonEmpty)
      .map(info => {
        val fields = info.split(",")
        val name = fields(0)
        val age = fields(1).toInt
        val height = fields(2).toInt
        val gender = fields(3)
        Person(name, age, height, gender)
      })

    val url = "jdbc:mysql://localhost:3306/test_db"
    val username = "root"
    val password = "123456"

    result.addSink(new MysqlSink(url, username, password))

    env.execute(this.getClass.getSimpleName)
  }

  class MysqlSink(url: String, username: String, password: String) extends RichSinkFunction[Person] {
    var con: Connection = _
    var statement: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      con = DriverManager.getConnection(url, username, password)
      statement = con.prepareStatement(
        """
          |insert into person
          |values(?,?,?,?)
          |""".stripMargin
      )
    }

    override def invoke(value: Person, context: SinkFunction.Context[_]): Unit = {
      statement.setString(1, value.name)
      statement.setInt(2, value.age)
      statement.setInt(3, value.height)
      statement.setString(4, value.gender)
      statement.executeUpdate()
    }

    override def close(): Unit = {
      if (statement != null) {
        statement.close()
      }
      if (con != null) {
        con.close()
      }
    }

  }

}
