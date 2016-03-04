package feature

import java.io.PrintWriter
import data.extract.{DataLoad}

object FeatLast {

  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println( "FeatLast")
    println("FeatLast Intro: stat last watch_cnt in the last three days of each video.")
    val feat_out = new PrintWriter(base_pt + "/" + feat_name )
    DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < week_id && week >= (week_id - interval)
    }.map{
      case ( uid, week, day, video_site, watch_count ) => ( (video_site , uid) ,( ( week - 1) * 7 +  day, watch_count) )
    }.groupBy( _._1 ).map{ x =>
      //      x._2.foreach( print )
      val ( vid, uid) = x._1
      val (last_day, last_cnt) = x._2.map{
        case ((vid, uid), (day, count))=>
          (day, count)
      }.sortBy(_._1).last
      var last = last_cnt
      if ((week_id - 1)* 7 - last_day > 3)
        last = 0
      Range( 0,7 ).map{
        r =>
          ( r, vid, uid, last)
      }
    }.flatMap( x => x).foreach{
      case ( r, vid, uid, last) =>
        feat_out.println( s"$r\t$vid\t$uid\t$last")
    }
    feat_out.close()
  }

  def main(args: Array[String]): Unit ={
    //val base_pt = "/Users/liping/Documents/tianyi/beautiful-gty/data/"
    val base_pt = args(0)
    extract( base_pt, "offline_train","feat_last.test", 7, 5)
    extract( base_pt, "offline_train", "feat_last.train",6 ,5 )
    extract( base_pt, "offline_train", "feat_last.pred",8 ,5 )
  }
}
