/**
 * @author ：kenor
 * @date ：Created in 2020/8/27 21:40
 * @description：
 * @version: 1.0
 */
class Pair[T: Ordering](x: T, y: T) {
  def lager(implicit ordering: Ordering[T]): T = {
    if (ordering.compare(x, y) > 0) x else y
  }
}

object ImplicitDemo extends App {
  val pair = new Pair[Int](10, 11)
  println(pair.lager)
}
