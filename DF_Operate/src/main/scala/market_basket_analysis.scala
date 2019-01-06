import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_set, count}
object market_basket_analysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("loanRisk")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val data_home = "E:\\资料\\ml\\ML-At-Scale-eBook-181029\\Market Basket Analysis\\instacart_online_grocery_shopping_2017_05_01\\instacart_2017_05_01"
    //# Import Data
    // row:134
    val aisles = spark.read.option("header",true).option("inferSchema",true).csv(data_home+"/aisles.csv").cache()
    // row:21
    val departments = spark.read.option("header",true).option("inferSchema",true).csv(data_home+"/departments.csv").cache()
    //row:3243 4489
    val order_products_prior = spark.read.option("header",true).option("inferSchema",true).csv(data_home+"/order_products__prior.csv").cache()
    //row:138 4617
    val order_products_train = spark.read.option("header",true).option("inferSchema",true).csv(data_home+"/order_products__train.csv").cache()
    //row:342 1083
    val orders = spark.read.option("header",true).option("inferSchema",true).csv(data_home+"/orders.csv").cache()
    //row:49688
    val products = spark.read.option("header",true).option("inferSchema",true).csv(data_home+"/products.csv").cache()

    print(aisles.count(),departments.count(),order_products_prior.count(),order_products_train.count(),orders.count(),products.count())
    // Create Temporary Tables
    aisles.createOrReplaceTempView("aisles")
    departments.createOrReplaceTempView("departments")
    order_products_prior.createOrReplaceTempView("order_products_prior")
    order_products_train.createOrReplaceTempView("order_products_train")
    orders.createOrReplaceTempView("orders")
    products.createOrReplaceTempView("products")

    //Busiest day of the week
    val busy_sql = "select count(order_id) as total_orders," +
      "(case   when order_dow = '0' then 'Sunday'" +
              "when order_dow = '1' then 'Monday'" +
              "when order_dow = '2' then 'Tuesday'" +
              "when order_dow = '3' then 'Wednesday'" +
              "when order_dow = '4' then 'Thursday'" +
              "when order_dow = '5' then 'Friday'" +
              "when order_dow = '6' then 'Saturday' " +
      "end) as day_of_week from orders  group by order_dow order by total_orders desc"
//    spark.sql(busy_sql).show()

    //Breakdown of Orders by Hour of the Day
    val hour_sql = "select count(order_id) as total_orders, order_hour_of_day as hour  from orders  group by order_hour_of_day  order by order_hour_of_day"
//    spark.sql(hour_sql).show()

    //Max Products by Department
    val pro_sql = "select countbydept.*\n  from (\n  -- from product table, let's count number of records per dept\n  -- and then sort it by count (highest to lowest) \n  select department_id, count(1) as counter\n    from products\n   group by department_id\n   order by counter asc \n  ) as maxcount\ninner join (\n  -- let's repeat the exercise, but this time let's join\n  -- products and departments tables to get a full list of dept and \n  -- prod count\n  select\n    d.department_id,\n    d.department,\n    count(1) as products\n    from departments d\n      inner join products p\n         on p.department_id = d.department_id\n   group by d.department_id, d.department \n   order by products desc\n  ) countbydept \n  -- combine the two queries's results by matching the product count\n  on countbydept.products = maxcount.counter"
//    spark.sql(pro_sql).show()

    //Top 10 Popular Items
    val top_sql = "select count(opp.order_id) as orders, p.product_name as popular_product\n  from order_products_prior opp, products p\n where p.product_id = opp.product_id \n group by popular_product \n order by orders desc \n limit 10"
//    spark.sql(top_sql).show()

    //Shelf Space by Department
    val space_sql = "select d.department, count(distinct p.product_id) as products\n  from products p\n    inner join departments d\n      on d.department_id = p.department_id\n group by d.department\n order by products desc\n limit 10"
//    spark.sql(space_sql).show()


    //---------------------------------------------------Train ML Model-------------------------------------------------
    //Organize and view Shopping Basket
    //Organize the data by shopping basket
    val rawData = spark.sql("select p.product_name, o.order_id from products p inner join order_products_train o where o.product_id = p.product_id")
    val baskets = rawData.groupBy("order_id").agg(collect_set("product_name").alias("items")).cache()
    baskets.createOrReplaceTempView("baskets")


    // use FP-growth
    // Extract out the items
    val baskets_ds = spark.sql("select items from baskets").as[Array[String]].toDF("items")

    // Use FPGrowth
    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.001).setMinConfidence(0)
    val model = fpgrowth.fit(baskets_ds)

    //Most Frequent Itemsets

    // Display frequent itemsets
    val mostPopularItemInABasket = model.freqItemsets
    mostPopularItemInABasket.createOrReplaceTempView("mostPopularItemInABasket")
    val popu_sql = "select items, freq from mostPopularItemInABasket where size(items) > 2 order by freq desc limit 20"
//    spark.sql(popu_sql).show()


    //View Generated Association Rules
    // Display generated association rules.
    val ifThen = model.associationRules
    ifThen.createOrReplaceTempView("ifThen")
    val rule_sql = "select antecedent as `antecedent (if)`, consequent as `consequent (then)`, confidence from ifThen order by confidence desc limit 20"
    spark.sql(rule_sql).show(truncate=false)
  }
}
