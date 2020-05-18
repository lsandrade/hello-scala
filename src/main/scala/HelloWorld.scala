import java.util.{Date, Locale}
import java.text.DateFormat._

object HelloWorld {
    def main(args: Array[String]): Unit = {
      val now = new Date
      val df = getDateInstance(LONG, Locale.ENGLISH)
      println("Hello World")

      println(df format now)

      println( 1 + 3)
    }
}