import org.apache.spark.sql.SparkSession

object create_DataFrame {
       def main(args:Array[String]):Unit={
         val spark: SparkSession = SparkSession.builder()
           .appName("garbage classfication")
           .master("local[1]")
           .getOrCreate()
         val sc = spark.sparkContext
         sc.setLogLevel("WARN")
         //list里面放tuple
         val df = spark.createDataFrame(Seq(
           (0,Seq("I", "saw", "the", "red", "balloon")),
           (1,Seq("Mary", "had", "a", "little", "lamb"))
         )).toDF("id","msg")
         df.show(false)

         val df0 = spark.createDataFrame(Seq(
           Array("a","b","c"),
           Array("d","e","f"),
           Array("g","h","i")
         ).map(Tuple1.apply(_))).toDF("text")
         df0.show(false)

         //list 里面放bean
         val list_bean = Array(
           Persion("Tom",8),
           Persion("Joy",35)
         )
         val df1 = spark.createDataFrame(list_bean)
         df1.show(false)


         var tuples: List[(Int, String, String, String, String, String)] =List[(Int, String, String, String, String, String)]()
         tuples = tuples.+:((0, "I", "saw", "the", "red", "balloon")).+:((1, "Mary", "had", "a", "little", "lamb"))
         val test = spark.createDataFrame(tuples).toDF("id","msg","1","2","3","4")
         test.show(false)
       }
  case class Persion(name:String,age:Int)
}
