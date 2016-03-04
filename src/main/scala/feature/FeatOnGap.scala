package feature

import java.io.PrintWriter

import data.extract.DataLoad
import org.saddle.Vec

/**
 * Created by Administrator on 2016/1/16.
 */
object FeatOnGap {

  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println( "FeatOnGap")
    val feat_out = new PrintWriter(base_pt + "/" + feat_name )

    val candidate  = DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < week_id && week >= (week_id - interval)
    }.map{
      case ( uid, week, day, video_site, watch_count ) => ( (uid , video_site) ,( ( week - 1) * 7 +  day, watch_count) )
    }

    candidate.groupBy( _._1 ).map{ x =>
      //_._1 =>��ʾ��ѡ�ı�ʶ�� _._2 =>( (uid, vid), (day, wcount) )

      val ( uid, vid) = x._1
      val visit_days = x._2.map( y => y._2._1 ).sorted

      val day = ( week_id - 1 ) * 7 + 1
      val left = Vec(  ( visit_days.toList:+ day ).toArray )
      val right = Vec( ( 0::visit_days.toList).toArray )

      val avg_gap = (left - right ).mean

      val last_day = x._2.map( y => y._2._1 ).sorted.max

      Range( 0,7 ).map{
        r =>
          val day = ( week_id - 1 ) * 7 + r + 1
          val day_gap = day - last_day
          //�������С��1,���ʾgap�����ڵ���
          if( ( day_gap - avg_gap*(day_gap/avg_gap).toInt ) < 1 )
            ( uid, vid, r, 1)
          else ( uid, vid, r, 0)
      }

    }.flatMap( x => x).foreach{
      case ( uid, vid, r,ongap ) =>
        feat_out.println( s"$uid\t$vid\t$r\t$ongap")
    }
    feat_out.close()
  }

  def main(args: Array[String]): Unit ={
    //val base_pt = "/Users/liping/Documents/tianyi/beautiful-gty/data/"
    val base_pt = args(0)
    extract( base_pt, "offline_train","feat_ongap.test", 7 ,5)
    extract(base_pt, "offline_train", "feat_ongap.train", 6,5 )
    extract(base_pt, "offline_train", "feat_ongap.pred", 8,5 )
  }
}
