import java.io.FileInputStream
import java.util.Properties

class utils {
  def getParam(key:String):String={
    val prop: Properties = getProp("city_map.properties")
    val value: String = prop.getProperty(key)
    value
  }
  def getProp(name:String):Properties={
    val prop = new Properties()
    val path = Thread.currentThread().getContextClassLoader().getResource(name).getPath
    prop.load(new FileInputStream(path))
    prop
  }
}
