package feature

import java.io.PrintWriter

import data.extract.DataLoad
import org.saddle.Vec

import scala.collection.immutable.IndexedSeq

/**
  * Created by Administrator on 2016/1/16.
  */
object FeatWeekDay {
   def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
     println( "FeatWeekDay")
     val feat_out = new PrintWriter(base_pt + "/" + feat_name )

     val begin_week = (week_id - interval)
     val end_week = week_id
     //获取候选集
     val candidate = DataLoad.load( base_pt + "/" + source_name ).filter{
       case ( uid, week, day, video_site, watch_count ) =>
         week < end_week && week >= begin_week
     }.map{
       case ( uid, week, day, video_site, watch_count ) => ( (uid , video_site) ,( week, watch_count) )
     }

     candidate.groupBy( _._1 ).map{ x =>
       //_._1 =>表示候选的标识， _._2 => Seq( (uid, vid), (week, wcount) )
       val ( uid, vid) = x._1

       //转换为每周一个列表
       val week_counts: Map[Int, Array[(Int, Int)]] = x._2.map{
         case ( (uid, vid), (week, count) ) =>
           (week, count)
       }.groupBy( _._1 )

       val feats: Map[Int, (Int, Int, Double, Double, Double)] = week_counts.map{
         x =>
           val week = x._1

           val counts: Array[Int] = x._2.map{
             case (week,cnt) =>
               cnt
           }

           val vec = Vec( counts )
           val cnt = vec.length
           val sum = vec.sum
           val median = vec.median
           val mean = vec.mean
           val stdev = vec.stdev
           ( week,( cnt, sum, median, mean, stdev ))
       }

       val week_feats = Range(begin_week,end_week).map{
         wk =>
           if( !feats.contains(wk ) ) (0,0,0,0,0,0)
           else{
             val x = feats(wk)
             (1, x._1,x._2,x._3,x._4,x._5)
           }
       }
       Range( 0,7 ).map{
         r =>
           ( uid, vid, r, week_feats )
       }

     }.flatMap( x => x).foreach{
       case ( uid, vid, r, week_feats ) =>
         feat_out.print( s"$uid\t$vid\t$r")
         week_feats.foreach{
           case (f1,f2,f3,f4,f5,f6) =>
             feat_out.print(s"\t$f1\t$f2\t$f3\t$f4\t$f5\t$f6")
 //            feat_out.print(s"\t$f1")
         }
         feat_out.println()
     }
     feat_out.close()
   }

   def main(args: Array[String]): Unit ={
     //val base_pt = "/Users/liping/Documents/tianyi/beautiful-gty/data/"
     val base_pt = args(0)
     extract(base_pt, "offline_train","feat_week1.test", 7 ,5)
     extract(base_pt, "offline_train", "feat_week1.train", 6,5 )
     extract(base_pt, "offline_train", "feat_week1.pred", 8,5 )
   }
 }
