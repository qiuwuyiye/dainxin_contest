package data.extract

import scala.io.Source

object DataLoad {

  def load( source_pt : String ): Array[(String,Int,Int,Int,Int)] ={
    Source.fromFile( source_pt ).getLines().map{ line =>
      val arr = line.trim().split("\\s+")
      val uid = arr(0)
      val week = arr( 1 ).toInt
      val day = arr(2).toInt
      val vedio_site = arr(3).toInt
      val click_count = arr(4).toInt

      ( uid, week, day, vedio_site, click_count)
    }.toArray
  }

  def main(arr: Array[String]){
    val base_pt = "/Users/liping/Documents/tianyi/beautiful-gty/data/"
    load(base_pt + "offline_train").foreach( x => println( x  ))

  }
}
