package feature

import java.io.PrintWriter

import data.extract.DataLoad

/**
  * Created by Administrator on 2016/1/16.
  */
object FeatOHEID {

   def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
     println( "FeatOHEID")
     println("FeatOHEID Intro: video id and day OHE encoding.?")
     val feat_out = new PrintWriter(base_pt + "/" + feat_name )

     //获取候选集
     val candidate = DataLoad.load( base_pt + "/" + source_name ).filter{
       case ( uid, week, day, video_site, watch_count ) =>
         week < week_id && week >= (week_id - interval)
     }.map{
       case ( uid, week, day, video_site, watch_count ) => ( (video_site, uid) ,( ( week - 1) * 7 +  day, watch_count) )
     }

     candidate.groupBy( _._1 ).map{ x =>
       //_._1 =>表示候选的标识， _._2 =>( (uid, vid), (day, wcount) )
       val ( vid, uid) = x._1
       Range( 0,7 ).map{
         r =>
           ( vid, uid, r)
       }

     }.flatMap( x => x).foreach{
       case ( vid, uid, r ) =>
         feat_out.println( s"$r\t$vid\t$uid\t$vid\t$r")

           //one-hot
           /*feat_out.print(s"$uid\t$vid\t$r")
          for( i <- 0 until 7 ){
            if( r == i ) feat_out.print("\t1")
            else feat_out.print("\t0")
          }

         for( i <- 1 to 10){
           if( vid == i ) feat_out.print("\t1")
           else feat_out.print("\t0")
         }*/
     }

     feat_out.close()
   }

   def main(args: Array[String]): Unit ={
     //extract( args(0), args(1), args(2), args(3).toInt, args(4).toInt )
     //val base_pt = "/Users/liping/Documents/tianyi/beautiful-gty/data/"
     val base_pt = args(0)
     extract(base_pt, "offline_train", "feat_idonehot.train", 6, 5)
     extract(base_pt, "offline_train", "feat_idonehot.test", 7, 5)
     extract(base_pt, "offline_train", "feat_idonehot.pred", 8, 5)

 //    extract( "E:/video_click/data/tianyi_bd_history_new","offline_train","feat_idonehot.test", 7 ,5)
 //    extract( "E:/video_click/data/tianyi_bd_history_new", "offline_train", "feat_idonehot.train", 6,5 )
 //    extract( "E:/video_click/data/tianyi_bd_history_new","online_train","online/feat_idonehot.test", 8 ,5)
 //    extract( "E:/video_click/data/tianyi_bd_history_new", "online_train", "online/feat_idonehot.train", 7,5 )
 //    extract( "E:/video_click/data/tianyi_bd_history_new","online_train","online/feat_id.test", 8 ,5)
 //    extract( "E:/video_click/data/tianyi_bd_history_new", "online_train", "online/feat_id.train", 7,5 )

   }

 }
