import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.util.LongAccumulator

object accumulator_trap {
  def main(args: Array[String]): Unit = {
    //累加器Accumulator的陷阱
   trap()

  }
  def cut_dependency(df:DataFrame)={
    val spark=SparkSession.builder()
      .master("local[2]")
      .appName("accumulator_trap")
      .getOrCreate()
    import spark.implicits._
    val sc: SparkContext = spark.sparkContext

    val df: DataFrame = spark.range(0,10).toDF("id")

    val accum: LongAccumulator = sc.longAccumulator("trap")
    //在未执行 action操作之前，累加器的值为0
    println(accum.value)

    //执行一次action操作后，累加器的值是正确的,accum.value=10
    val new_df: Dataset[Long] = df.map(x=>{accum.add(1);x.getLong(0)})

    //使用cache缓存数据，切断依赖,action操作都放在cache_df中使用
    val cache_df=new_df.cache()
    cache_df.count()
    println(accum.value)

    //new_df.show()
    cache_df.foreach(println(_))
    println(accum.value)
  }
  def trap()={
    /**
      * 本次有两个action，说明有两个job，
      * 第一个job执行，执行map操作，累加器累加一遍，然后show
      * 第二个job执行，也执行map操作，累加器累加一遍，然后再执行foreach
      * 这是因为两个action都依赖于map操作
      */
    val spark=SparkSession.builder()
      .master("local[2]")
      .appName("accumulator_trap")
      .getOrCreate()
    import spark.implicits._
    val sc: SparkContext = spark.sparkContext

    val df: DataFrame = spark.range(0,10).toDF("id")
    val accum: LongAccumulator = sc.longAccumulator("trap")
    //在未执行 action操作之前，累加器的值为0
    println(accum.value)

    //执行一次action操作后，累加器的值是正确的,accum.value=10
    val new_df: Dataset[Long] = df.map(x=>{accum.add(1);x.getLong(0)})
    new_df.count()
    println("first action:"+accum.value)

    //再执行一次action操作，累加器的值又累加了一遍,accum.value=20

    //    new_df.show()
    new_df.foreach(println(_))
    println("second action:"+accum.value)
  }

}
