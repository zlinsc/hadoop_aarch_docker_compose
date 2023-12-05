package lakepump.demo

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object NetcatDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
    val src = env.socketTextStream("localhost", 9000)
    val out = src.flatMap(new FlatMapFunction[String, String] {
      override def flatMap(t: String, collector: Collector[String]): Unit = {
        val arr = t.split(",")
        for (w <- arr) collector.collect(w)
      }
    }).map(x => x.toLowerCase())
    out.print()
    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}
