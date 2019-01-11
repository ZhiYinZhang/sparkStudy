
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import scala.collection.mutable.ListBuffer
object demo1 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("demo")
      .master("local[3]")
      .config("spark.ui.port","36000")
//      .config("spark.speculation",true)
//      .config("spark.serializer","org.apache.spark.serializer.kryoSerializer")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    sc.setLogLevel("WARN")

    val data_home = "E:\\资料\\ml\\ML-At-Scale-eBook-181029\\Market Basket Analysis\\instacart_online_grocery_shopping_2017_05_01\\instacart_2017_05_01"
    //# Import Data
    // row:134
    val aisles = spark.read.option("header",true).option("inferSchema",true).csv(data_home+"/aisles.csv").cache()
    // row:21
    val departments = spark.read.option("header",true).option("inferSchema",true).csv(data_home+"/departments.csv").cache()
    //row:3243 4489
//    val order_products_prior = spark.read.option("header",true).option("inferSchema",true).csv(data_home+"/order_products__prior.csv").cache()
    //row:138 4617
    val order_products_train = spark.read.option("header",true).option("inferSchema",true).csv(data_home+"/order_products__train.csv").cache()
    //row:342 1083
    val orders = spark.read.option("header",true).option("inferSchema",true).csv(data_home+"/orders.csv").cache()
    //row:49688
    val products = spark.read.option("header",true).option("inferSchema",true).csv(data_home+"/products.csv").cache()

    //print(aisles.count(),departments.count(),order_products_prior.count(),order_products_train.count(),orders.count(),products.count())
    // Create Temporary Tables
    aisles.createOrReplaceTempView("aisles")
    departments.createOrReplaceTempView("departments")
//    order_products_prior.createOrReplaceTempView("order_products_prior")
    order_products_train.createOrReplaceTempView("order_products_train")
    orders.createOrReplaceTempView("orders")
    products.createOrReplaceTempView("products")

    val sql="select o.*,opt_p.product_name,opt_p.aisle,opt_p.department,opt_p.add_to_cart_order,opt_p.reordered from orders o inner join " +
                     "(select * from order_products_train opt inner join " +
                                        "(select * from products p inner join aisles a inner join departments d on p.aisle_id=a.aisle_id and p.department_id=d.department_id) as p_a_d " +
                     "on opt.product_id=p_a_d.product_id) as opt_p " +
            "on o.order_id = opt_p.order_id "
//    spark.sql(sql).show()

    val df: DataFrame = spark.sql(sql)

    df.repartition(1).write.format("csv")
      .option("header",true)
      .save("e://test/market_basket")


  }

}
