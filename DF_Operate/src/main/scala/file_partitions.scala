object file_partitions {
  def main(args: Array[String]): Unit = {
    val files=List(180633692, 180592610, 180931551, 180936221)
    val result=get_split(files)

    result.foreach{x=>
      println(x)
      println(x.length)
    }

  }
  def get_split(files:List[Int]):List[List[(Long,Long)]]={
    /**
      * 1.先计算maxSplitBytes
      * 2.然后遍历目录下文件，
      * 每个文件按照maxSplitBytes去split，split多少份就多少个分区，不足maxSplitBytes的也作为一个分区
      *
      */

    val defaultMaxSplitBytes = 128*1024*1024
    val openCostInBytes = 4*1024*1024
    val defaultParallelism = 200

    val totalBytes =  files.map(_+openCostInBytes).sum

    val bytesPerCore = totalBytes / defaultParallelism

    val maxSplitBytes = Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))


    println(totalBytes,bytesPerCore,maxSplitBytes)

    val result: List[List[(Long, Long)]] = files.map { file => //遍历文件
      val offsets: List[Long] = (0L until file by maxSplitBytes).toList //将文件按照maxSplitBytes切分
      offsets.map { offset =>
        //剩余数据量
        val remaining = file - offset
        //如果剩余量>maxSplitBytes,分区数据=maxSplitBytes；否则将剩余的作为一个分区的数据
        val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining
        (offset, size)
      }
    }
    result
  }
}
