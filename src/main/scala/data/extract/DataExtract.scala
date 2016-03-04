package data.extract

import scala.io.Source
import java.io.PrintWriter
/**
 * Created by Administrator on 2016/1/12.
 */
object DataExtract {

  def extract(data_pt:String): Array[(String, Int, Int, Int, Int)] = {
    Source.fromFile( data_pt ).getLines().map{ line =>
      val arr = line.split("\\s+")
      val uid = arr(0)
      val date_str = arr(1).replace('w', ' ').replace('d',' ').trim.split( "\\s+")
      val week = date_str( 0 ).toInt
      val day = date_str(1).substring(0, 1).toInt
      val video_site = arr(2).replace('v', ' ').trim.toInt
      val click_count = arr(3).toInt
      ( (uid, week, day, video_site), click_count)
    }.toArray.groupBy(_._1).map{
      case x =>
        val (uid, week, day, video_site) = x._1
        val sum = x._2.map(_._2).sum
        (uid, week, day, video_site, sum)
    }.toArray
  }

  def main(args: Array[String]){
    //val base_pt = "/Users/liping/Documents/tianyi/beautiful-gty/data/"
    val base_pt = args(0)
    val source_pt = "offline_train"
    val offline = new PrintWriter(base_pt + source_pt)
    extract(base_pt + "/part-r-00000").foreach{
      case (uid, week, day, video, cnt) =>
        offline.println(s"$uid\t$week\t$day\t$video\t$cnt")
    }
    offline.close()
  }
}



