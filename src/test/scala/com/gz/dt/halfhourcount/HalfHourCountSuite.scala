package com.gz.dt.halfhourcount

import com.gz.dt.halfhourcount.HalfHourCount
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite

/**
 * Created by naonao on 2016/1/21.
 */
class HalfHourCountSuite extends FunSuite {

  test("check HalfHourCount") {
    val conf = new SparkConf().setAppName("HalfHourCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = HalfHourCount.stat(Array("data/CDMA_1X_test.normal", "20150409"), sc)
    val arr = rdd.collect()

    //result: [(26,5),(27,3),(28,1),(29,1)]
    assert(arr.length === 4)
    assert(arr(0) === (26, 5))
    assert(arr(1) === (27, 3))
    assert(arr(2) === (28, 1))
    assert(arr(3) === (29, 1))

    sc.stop()
  }
}
