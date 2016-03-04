package preprocess

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

/**
 * Created by liping on 2/27/16.
 */
object SortTwoData {

  def main(args: Array[String]): Unit =
  {
    //read two files
    val conf = new SparkConf().setAppName("Compare Data")
    //.setMaster("local[2]")
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val train_pt = args(0)
    val label_pt = args(1)
    sc.textFile(train_pt)
      .union(sc.textFile(label_pt))
    .map {
      line => line.split("\\s+") match {
        case Array(user, date, video, cnt) => ((user + "\t" + date), (video, cnt))
        case _ =>
          val length = line.split("s\\+").length
          println(s"can not parse this format ${line}, length is ${length}, so skip it.")
          ("a", "1")
      }
    }.sortBy(_._1).saveAsTextFile(args(2))

    sc.stop()
  }
}
