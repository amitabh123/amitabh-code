
package test

import mux.db._
import mux.db.core.DataStructures._
import mux.db.core._
import CommonTest._
import BetterDB._

object TestJoin extends App {
  import Main._
  case class Person(uid:String, email:String, value:BigInt)
  def arrayToPerson(a:Array[Any]) = {
    Person(a(0).asInstanceOf[String], a(1).asInstanceOf[String], a(2).asInstanceOf[BigInt])
  }
  val (uid, email, value) = (Col("uid", VARCHAR(255)), Col("email", VARCHAR(255)), Col("value", UScalaBIGINT(100)))
  val table1 = Table("A1", Array(uid, email, value), Array(uid))
  val table2 = Table("A2", Array(uid, value), Array(value))
  val dbm1 = new DBManager(table1)(MyDBConfig)
  val dbm2 = new DBManager(table2)(MyDBConfig)  
  foo
  def foo = usingExit {
    println(" Create table 1: "+dbm1.getTable.createSQLString)
    println(" Create table 2: "+dbm2.getTable.createSQLString)
    dbm1.insert("amit", "hello", BigInt(9223372036854775806L))
    dbm1.insert("guru", "world", 1l)
    dbm1.insert("ravi", "hi", 51222)
    dbm2.insert("ravi", 1)
    dbm2.insert("ravi", 2)
    dbm2.insert("amit", BigInt(9223372036854775806L))
    dbm2.insert("ravi", 51222)
    dbm2.insert("guru", 11)
    dbm2.deleteWhere(value === 1)
    println("======")
    dbm1.select(uid.of(dbm1), email, value).as(arrayToPerson) foreach println
    println("======")
    dbm1.select(uid, email, value).where(uid like "ami%").as(arrayToPerson) foreach println
    println("======")
    dbm1.select(uid, email, value.of(dbm2)).where(value.of(dbm1) === value.of(dbm2)).as(arrayToPerson) foreach println
    println("======")
    dbm1.select(uid, email, value.of(dbm2)).where(
      (value.of(dbm1) >= value.of(dbm2)),
      (value.of(dbm1) <> value.of(dbm2) * (value.of(dbm1) + 5))
    ).as(arrayToPerson) foreach println
  } 
}















