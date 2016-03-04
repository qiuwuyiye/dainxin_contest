package feature

import java.io.PrintWriter

import data.extract.DataLoad

/**
 * Created by Administrator on 2016/1/16.
 */
object FeatID {

  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println( "FeatID")
    val feat_out = new PrintWriter(base_pt + "/" + feat_name )

    //获取候选集
    val candidate = DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < week_id && week >= (week_id - interval)
    }.map{
      case ( uid, week, day, video_site, watch_count ) => ( (uid , video_site) ,( ( week - 1) * 7 +  day, watch_count) )
    }

    candidate.groupBy( _._1 ).map{ x =>
      //_._1 =>表示候选的标识， _._2 =>( (uid, vid), (day, wcount) )
      val ( uid, vid) = x._1
      Range( 0,7 ).map{
        r =>
          ( uid, vid, r)
      }

    }.flatMap( x => x).foreach{
      case ( uid, vid, r ) =>
        feat_out.println( s"$uid\t$vid\t$r\t$vid\t$r")
    }
    feat_out.close()
  }

  def main(args: Array[String]): Unit ={
    //val base_pt = "/Users/liping/Documents/tianyi/beautiful-gty/data/"
    val base_pt = args(0)
    extract( base_pt,"offline_train","feat_id.test", 7 ,5)
    extract( base_pt, "offline_train", "feat_id.train", 6 ,5 )
    extract( base_pt, "offline_train", "feat_id.pred", 8 ,5 )
  }

}
