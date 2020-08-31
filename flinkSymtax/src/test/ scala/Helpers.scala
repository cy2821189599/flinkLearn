object Helpers {

  implicit class IntWithTimes(x: Int) {
    def times[A](f: => A): Unit = {
      def loop(current: Int): Unit =
        if (current > 0) {
          f
          loop(current - 1)
        }
      //call this function
      loop(x)
    }
  }

}

object Demo extends App {

  import Helpers._

  5 times println("hi")
}