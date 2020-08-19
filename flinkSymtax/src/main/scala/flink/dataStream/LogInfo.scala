package flink.dataStream

class LogInfo(id: Int, time: Int, content: String) {
  def this(id: Int) {
    this(id, 0, null)
  }

  override def toString(): String = {
    "id=" + id + ",time=" + time + ",content=" + content
  }
}