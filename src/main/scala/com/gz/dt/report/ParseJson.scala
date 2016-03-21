package com.gz.dt.report

import org.json4s._
import org.json4s.native.JsonMethods._

import scala.io.Source
import scala.collection.mutable.{Map => MMap}

/**
 * Created by naonao on 2016/3/16
 */

case class TargetTable(account: String, table_name: String, table_fields: List[String], primary_key: String, generate_frequency: String)
case class SourceTable(tablename_fieldname: List[String])
case class MasterTable(account: String, table_name: String, where_condition: String, is_distinct: String)
case class SlaveTable(account: String, tables: List[JoinCondition])
case class SlaveSlaveTable(account: String, tables: List[JoinCondition])
case class JoinCondition(table_name: String, join_mode: String, join_condition: String, where_condition: String, join_fields: List[String], is_distinct: String)
case class ReportConf(target_table: TargetTable, source_table: SourceTable, master_table: MasterTable, slave_table: SlaveTable, slave_slave_table: SlaveSlaveTable)


object ParseJson {

  def main(args: Array[String]) {
    val f = Source.fromFile("data/reportcong.json").mkString

    val j = parse(f)

    for((k,v) <- j.values.asInstanceOf[Map[String, _]]){
      println(s"$k -> $v")
    }

    implicit val formats = DefaultFormats
    val ss = j.extract[ReportConf]
    println(s"${ss.target_table.table_name} ${ss.master_table.table_name} ${ss.slave_table.account} ${ss.slave_slave_table.account}")

  }

}

class ParseJson {

}
