import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object EmulatedDemo {
  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val flintstones = env.fromElements(Person("apple", 35), Person("banana", 20), Person("cat", 12))
    val adults = flintstones.filter(_.age >= 18)
    adults.print()
    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}