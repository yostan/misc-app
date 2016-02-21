package com.gz.dt.topn

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by naonao on 2016/2/19.
 */
object TopApp {

  def main(args: Array[String]) {

    if(args.length != 3){
      System.err.println("Usage: TopApp <appType> <inputDataDir> <outputDataDir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("APP_TopN").setMaster("local[2]")
    conf.set("spark.testing.reservedMemory","0")   //only local test
    val sc = new SparkContext(conf)

    val appType = args(0)

    val rdd1 = if(appType == "-1"){   //统计所有类型排名
      sc.textFile(args(1), 2).map(x => x.split("\t")).filter(arr => arr.size == 4)
    } else {
      sc.textFile(args(1), 2).map(x => x.split("\t")).filter(arr => arr.size == 4 && arr(0) == appType)
    }

    val rdd2 = rdd1.map(x => (x(1) + "|" + x(2) + "|" + x(3), 1)).reduceByKey(_ + _).sortBy(m => m._2, false, 1).filter(x => x._2 > 1)

    rdd2.take(10).foreach(println)  //debug

    rdd2.saveAsTextFile(args(2))

    sc.stop()

  }
}
