import org.apache.spark.sql.SparkSession

object read_image {
    def main(args:Array[String]):Unit={
      val spark: SparkSession = SparkSession.builder()
        .master("local[2]")
        .appName("read_image")
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")

      val image_df = spark.read.format("image").load("E:\\Zhang_zhiyin\\buffet甜甜圈")
      image_df.printSchema()
      image_df.select("image.origin","image.width","image.height","image.nChannels","image.mode")
        .show(false)


    }
}
