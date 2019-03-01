package preOperator

import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.Vector
object normalize {
  def main(args: Array[String]): Unit = {
    new Vector {
      override def size: Int = ???

      override def toArray: Array[Double] = ???

      override def foreachActive(f: (Int, Double) => Unit): Unit = ???

      override def numActives: Int = ???

      override def numNonzeros: Int = ???

      override def argmax: Int = ???
    }
  }
}
