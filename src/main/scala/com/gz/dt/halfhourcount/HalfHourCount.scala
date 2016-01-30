package com.gz.dt.halfhourcount

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable

/**
 * Created by naonao on 2016/1/21.
 */
object HalfHourCount {

  def stat(args: Array[String], sc: SparkContext): RDD[(Int, Int)] = {
    if(args.length < 2){
      System.err.println("Usage: HalfHourCount <dataInPath> <date> [dataOutputPath] ... date format: 20150402(yyyyMMdd)")
      System.exit(1)
    }
    val rdd1 = sc.textFile(args(0), 2)
    val rdd2 = rdd1.map(_.split("\\%")).map(x => (x(0), x(7), x(8)))

    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val sd = args(1) + "000000"
    val ed = args(1) + "235959"
    val start_day = dateFormat.parse(sd).getTime / 1000
    val end_day = dateFormat.parse(ed).getTime / 1000

    val rdd3 = rdd2.mapPartitions { iter =>
      val treeSet = new mutable.TreeSet[(String, Int)]
      while (iter.hasNext) {
        val elem = iter.next()
        val startVisit = dateFormat.parse(elem._2).getTime / 1000
        val endVisit = startVisit + elem._3.toLong
        var sIndex = ((startVisit - start_day) / 1800).toInt

        if (sIndex >= 0 && endVisit <= end_day) {
          val eIndex = ((endVisit - start_day) / 1800).toInt
          while (sIndex <= eIndex) {
            treeSet.add(elem._1, sIndex)
            sIndex += 1
          }
        } else if (sIndex >= 0 && endVisit > end_day) {
          val eIndex = 47
          while (sIndex <= eIndex) {
            treeSet.add(elem._1, sIndex)
            sIndex += 1
          }
        }
      }
      treeSet.iterator
    }

    val rdd4 = rdd3.distinct().map(t => (t._2, 1)).reduceByKey(_ + _)
    rdd4.sortByKey(true, 1)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("HalfHourCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = stat(args, sc)
    rdd.saveAsTextFile(args(2))

    sc.stop()
  }
}
