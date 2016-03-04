package sample

import java.io.PrintWriter

import data.extract.{DataLoad, DataExtract}
import org.saddle.Vec

/**
 * Created by Administrator on 2016/1/13.
 */
object SampleBuild {
  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println("SampleBuild")
    val feat_out = new PrintWriter(base_pt + "/" + feat_name )

    //获得用户(uid,video_site)的行为列表
    val action_list = DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week <= week_id && week >= (week_id - interval)
    }.map{
      case ( uid, week, day, video_site, watch_count ) => ( (video_site , uid) ,( ( week - 1) * 7 +  day, watch_count) )
    }

    //获取有效的(uid, video_size)对
    val val_candidates: Map[(Int, String), Array[((Int, String), (Int, Int))]] = action_list.groupBy( _._1 ).filter {
      x =>
        val samples = x._2.map( y => y._2 )

        val day_threshold = ( week_id - 1 )*7
        val history = samples.filter{
          case ( day, count ) =>
            day <= day_threshold
        }
        history.size != 0
    }

    val_candidates.map { x =>

      val (vid, uid) = x._1
      //(day,count)
      val samples = x._2.map(y => y._2)

      val day_threshold = (week_id - 1) * 7

      val target = samples.filter {
        case (day, count) =>
          day > day_threshold
      }.map{
        case (day, count) =>
          ( day - ( day_threshold + 1 ), count )
      }

      val targetMap = target.toMap
      val entry = Range(0, 7).map {
        r =>
          if( targetMap.contains( r ))
            (uid, vid, r, targetMap(r))
          else
            (uid, vid, r, 0)
      }
      entry
    }.flatMap( x => x).foreach{
      case ( uid, vid, r, target ) =>
        feat_out.println( s"$r\t$vid\t$uid\t$target")
    }
    feat_out.close()
  }

  def main(args: Array[String]): Unit ={
    //val base_pt = "/Users/liping/Documents/tianyi/beautiful-gty/data/"
    val base_pt = args(0)
    extract( base_pt, "offline_train", "offline_label.train", 6 , 5)
    extract( base_pt, "offline_train", "offline_label.test", 7 , 5)
  }
}
