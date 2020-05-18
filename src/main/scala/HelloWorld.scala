import java.util.{Date, Locale}
import java.text.DateFormat._

object HelloWorld {
    def oncePerSecond(callback: () => Unit): Unit = {
      while (true) { callback(); Thread sleep 1000}
    }

    def main(args: Array[String]): Unit = {
      val now = new Date
      val df = getDateInstance(LONG, Locale.ENGLISH)

      // Hello world
      println("Hello World")

      // Format date
      println(df format now)

      // sum values
      println( 1 + 3)

      // call anonymous function
//      oncePerSecond(() => println("time flies like an arrow..."))

      // using classes
      val c = new Complex(1.2, 3.4)
      println("Imaginary part: " + c.im())

      // overriding toString method
      println("Overriden toString(): " + c.toString)
    }
}