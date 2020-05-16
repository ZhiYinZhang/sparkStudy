import java.lang.management.{ManagementFactory, MemoryMXBean, MemoryPoolMXBean}
import java.util

object jvm_info {
  def main(args: Array[String]): Unit = {
    val runtime: Runtime = Runtime.getRuntime

    val maxMemory = runtime.maxMemory()
    println("maxMemory :",maxMemory/1024/1024)

    val bean: MemoryMXBean = ManagementFactory.getMemoryMXBean

    println("Non-heap :",bean.getNonHeapMemoryUsage.getMax)
    println("heap :",bean.getHeapMemoryUsage.getMax)


    val beans: util.List[MemoryPoolMXBean] = ManagementFactory.getMemoryPoolMXBeans

    for(i<- 0 until beans.size()){
      val b = beans.get(i)
      println("Pool: " + b.getName() + " (type " + b.getType() + ")" + " = " + b.getUsage().getMax())
    }


    val m=Map("1"->"a","2"->"b")

    m.foreach(println(_))

  }
}
