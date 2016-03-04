package feature

import java.io.PrintWriter

import data.extract.DataLoad
import org.saddle.Vec

/**
 * Created by Administrator on 2016/1/14.
 */
object FeatAvgGap {
  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println("FeatAvgGap")

    val feat_out = new PrintWriter(base_pt + "/" + feat_name )
    DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < week_id && week >= (week_id - interval)
    }.map{
      case ( uid, week, day, video_site, watch_count ) => ( (uid , video_site) ,( ( week - 1) * 7 +  day, watch_count) )
    }.groupBy( _._1 ).map{ x =>
      //      x._2.foreach( print )
      val ( uid, vid) = x._1
      val visit_days: Array[Int] = x._2.map( y => y._2._1 ).sorted

      val day = ( week_id - 1 ) * 7 + 1
      val left = Vec(  ( visit_days.toList:+ day ).toArray )
      val right = Vec( ( 0::visit_days.toList).toArray )

      val avg_gap = (left - right ).mean
      val min_gap = (left - right ).min.getOrElse(0)
      val max_gap = (left - right ).max.getOrElse(35)
      val stdev_gap = (left - right ).stdev

      Range( 0,7 ).map{
        r =>
          ( uid, vid, r, avg_gap)
      }

    }.flatMap( x => x).foreach{
      case ( uid, vid, r, avg_gap) =>
        feat_out.println( s"$uid\t$vid\t$r\t$avg_gap")
    }
    feat_out.close()
  }

  def main(args: Array[String]): Unit ={
    //val base_pt = "/Users/liping/Documents/tianyi/beautiful-gty/data/"
    val base_pt = args(0)
    extract( base_pt,"offline_train","feat_avggap.test", 7 , 5)
    extract( base_pt, "offline_train", "feat_avggap.train", 6 , 5)
    extract( base_pt, "offline_train", "feat_avggap.pred", 8 , 5)
  }
}
