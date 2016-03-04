package label

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable
import scala.collection.immutable.IndexedSeq

/**
 * Created by liping on 3/2/16.
 */
object LabelStatFeat {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("LabelClassStat")
    val sc = new SparkContext(conf)

    val label_map: Map[String, Int] = Array(("休闲娱乐", 1),
      ("门户", 2),
      ("视频", 3),
      ("科技", 4),
      ("阅读", 5),
      ("政治", 6),
      ("生活", 7),
      ("购物", 8),
      ("教育", 9),
      ("体育",10),
      ("财经", 11),
      ("音乐", 12),
      ("游戏", 13),
      ("非法网站", 14),
      ("新闻", 15)
    ).toMap

    /**
     * Extract user_id, week, day, label(0), cnt
     * @param base_pt
     */
    def extract_raw(base_pt: String): RDD[(String, Int, Int, Int, Int)] =
    {
      sc.textFile(base_pt).filter{
        line =>
          val t: Array[String] = line.split("\\s+")
          t.length == 4
      }.map{
        ln =>
          val Array(uid, date, label, cnt) = ln.split(s"\\t")
          val week = date.substring(1, 2).toInt
          val day = date.substring(3, 4).toInt
          val label_0 = label.split(",").head
          val count = cnt.toInt
          val label_mirror = if(label_map.contains(label_0)) label_map(label_0) else 0
          (uid, week, day, label_mirror, count)
      }
    }

    /**
     * Extract data keys.
     * @param base_pt
     * @return
     */
    def extract_visit_data(base_pt: String, week_id: Int, interval: Int): RDD[((Int, String), Int)] = {
      sc.textFile(base_pt).map {
        line =>
          val Array(uid, date, video, cnt) = line.split("\\s+")
          val week = date.substring(1, 2).toInt
          val day = date.substring(3, 4).toInt
          val v = video.substring(1).toInt
          (day, uid, week, v)
      }.cache().filter {
        case (day, uid, week, v) =>
          week < week_id && week >= week_id - interval
      }.map {
        case (day, uid, week, v) =>
          (v, uid)
      }.distinct().map {
        ln =>
          val (vid, uid) = ln
          Range(1, 8).map {
            r =>
              ((r, uid), vid)
          }
      }.flatMap(x => x)
    }

    def extract(raw: RDD[(String, Int, Int, Int, Int)], visit_pt: String, interval:Int, week_id:Int, res_pt: String): Unit =
    {
      val data_filter = extract_visit_data(visit_pt, week_id, interval).cache()
      println(s"week_id: ${week_id}, interval: ${interval}, count: ${data_filter.count}")
      val user_day_info = raw.filter{
        case (uid, week, day, label, cnt) =>
          week < week_id &&  week >= (week_id - interval)
      }.map{
        case (uid, week, day, label, cnt) =>
          ((uid, day), (label, week, cnt))
      }.groupBy(_._1).map{
        case ((uid, day), iter) =>

          val target: Map[Int, Int] = iter.map(_._2).map{
            tuple =>
              (tuple._1, tuple._3)
          }.toArray.groupBy(_._1).map{
            case a =>
              (a._1, a._2.map(_._2).sum)
          }

          val x1 = Range(0, 16).map{
            pointer =>
              if(target.contains(pointer))
                target(pointer)
              else
                0
          }.toArray
          ((day, uid), x1)
      }
      data_filter.leftOuterJoin(user_day_info).map{
        case ((day, uid), (vid, x1)) =>
          val x: Array[Int] = x1.getOrElse(Array.fill(16)(0))
          s"${day}\t${vid}\t${uid}\t${x.mkString("\t")}"
      }.saveAsTextFile(res_pt)
    }

    val base_pt="hdfs://bda00:8020/user/houjp/tianyi/data/raw/"
    val b_pt=base_pt + "user-behavior-data"
    val v_pt=base_pt + "video-visit-data.txt"
    val liping_pt = "hdfs://bda00:8020/user/liping/tianyi/"
    val raw_label: RDD[(String, Int, Int, Int, Int)] = extract_raw(b_pt).cache()
    extract(raw_label, v_pt, 5, 6, liping_pt + "label.train")
    extract(raw_label, v_pt, 5, 7, liping_pt + "label.test")
    extract(raw_label, v_pt, 5, 8, liping_pt + "label.pred")
    sc.stop()
  }
}
