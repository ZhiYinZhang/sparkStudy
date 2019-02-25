import java.text.SimpleDateFormat
import java.util.{Date, UUID}


object city_map {
  def main(args: Array[String]): Unit = {
    val record = "1.0,113.71593744459359,26.11232263028814,2018-12-04-17-23-19"

   println(" 1".toDouble)
   println("1.01".toDouble)


  }
  def gene_data(record:String):Array[String]={
    val records: Array[String] = record.split(",")

    val count = records(0).toInt
    val wgs_lng=records(1).toDouble
    val wgs_lat=records(2).toDouble
    val data_time=records(3)

    val id = UUID.randomUUID().toString()
    val add_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())


    records
  }
}
