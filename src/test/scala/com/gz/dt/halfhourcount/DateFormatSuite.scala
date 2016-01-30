package com.gz.dt.halfhourcount

import java.text.SimpleDateFormat

import org.scalatest.FunSuite

/**
 * Created by naonao on 2016/1/21.
 */
class DateFormatSuite extends FunSuite {

  test("java SimpleDateFormat") {
    val dateFormat1 = new SimpleDateFormat("yyyyMMddHHmmss")
    val dateFormat2 = new SimpleDateFormat("yyyyMMdd")
    val d1 = dateFormat1.parse("20150409000000").getTime / 1000
    val d2 = dateFormat2.parse("20150409").getTime / 1000
    assert(d1 === d2)
  }

}
