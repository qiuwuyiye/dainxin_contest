package feature

import java.io.PrintWriter
import data.extract.{DataLoad}

object FeatWeekSum {

  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println( "FeatWeekSum")
    println("FeatWeekSum Intro: last seven days watch cnt sum and identity.")
    val feat_out = new PrintWriter(base_pt + "/" + feat_name )
    DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < week_id && week >= (week_id - interval)
    }.map{
      case ( uid, week, day, video_site, watch_count ) => ( (video_site, uid) ,( ( week - 1) * 7 +  day, watch_count) )
    }.groupBy( _._1 ).map{ x =>
      //      x._2.foreach( print )
      val ( vid, uid) = x._1
      val last_week = x._2.filter{
        case ((vid, uid), (day, count))=>
          day > (week_id - 2) * 7
      }.map(_._2._2)
      val last_week_sum = last_week.sum
      val last_week_cnt = last_week.size
      val watch_or_not = if(last_week_cnt > 0) 1 else 0
      Range( 0,7 ).map{
        r =>
          ( r, vid, uid, last_week_sum, watch_or_not)
      }
    }.flatMap( x => x).foreach{
      case ( r, vid, uid, last_week_sum, watch_or_not) =>
        feat_out.println( s"$r\t$vid\t$uid\t$last_week_sum")
    }
    feat_out.close()
  }

  def main(args: Array[String]): Unit ={
    //val base_pt = "/Users/liping/Documents/tianyi/beautiful-gty/data/"
    val base_pt = args(0)
    extract( base_pt, "offline_train","feat_week_sum.test", 7 ,5)
    extract( base_pt, "offline_train", "feat_week_sum.train", 6,5 )
    extract( base_pt, "offline_train", "feat_week_sum.pred", 8,5 )
  }
}

