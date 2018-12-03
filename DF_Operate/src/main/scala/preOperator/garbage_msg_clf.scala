package preOperator

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, LabeledPoint}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object garbage_msg_clf{
    def main(args:Array[String]):Unit={
      val spark: SparkSession = SparkSession.builder()
        .appName("garbage classfication")
        .master("local[1]")
        .getOrCreate()
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")
      import spark.implicits._

      val spam =spark.read.text("E:\\test\\ml\\spam.txt")
      val normal = spark.read.text(path="e:\\test\\ml\\normal.txt")



      val spam_split = spam.map(_.getString(0).split(" ")).toDF("text")
      val normal_split = normal.map(_.getString(0).split(" ")).toDF("text")

      //创建一个HashingTF实例来把邮件文本映射为包含10000个特征的向量
      val tf = new HashingTF().setNumFeatures(10000)
          .setInputCol("text")
          .setOutputCol("features")
      val spam_features = tf.transform(spam_split)
      val normal_features = tf.transform(normal_split)

      val positiveExample = spam_features.map(email=> LabeledPoint(1,email.getAs[Vector](1)))
      val negativeExample = normal_features.map(email=>LabeledPoint(0,email.getAs[Vector](1)))

     val trainData = positiveExample.union(negativeExample)
      //因为逻辑回归是迭代算法，缓存训练数据
      trainData.persist(StorageLevel.MEMORY_AND_DISK)
      val model = new LogisticRegression().fit(trainData)


      val document_test = spark.createDataFrame(Seq(
        "O M G GET cheap stuff by sending money to ...".split(" "),
        "Hi Dad,I started studying Spark the other ...".split(" ")
      ).map(Tuple1.apply(_))).toDF("text")
      val test_data = tf.transform(document_test).toDF("text","features")
      test_data.show(false)
      model.transform(test_data).show(false)

    }
}
case class Persion(name:String,message:String)
class garbage_msg_clf {
}
