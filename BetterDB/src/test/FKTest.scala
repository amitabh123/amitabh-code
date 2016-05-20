
package test

import CommonTest._
import mux.db.core.DataStructures._
import mux.db.core._
import mux.db.BetterDB._

import mux.db.core.FKDataStructures._
import mux.db.{DBManager => DBMgr}

object FKTest {
  
  def main(args:Array[String]):Unit = {}
  case class Person(uid:String, email:String, bal:BigInt)
  def arrayToPerson(a:Array[Any]) = Person(a(0).asInstanceOf[String], a(1).asInstanceOf[String], a(2).asInstanceOf[BigInt])
  usingExit {
    val (uid, email, bal) = (Col("uid", VARCHAR(255)), Col("email", VARCHAR(2)), Col("value", UScalaBIGINT(100)))
    val (iid, desc) = (Col("iid", VARCHAR(255)), Col("desc", VARCHAR(255)))

    val (oid, amt) = (Col("oid", VARCHAR(255)), Col("amt", VARCHAR(255)))

    val users = Table("usrTT3", Array(uid, email, bal), Array(uid))
    val orders = Table("ordTT3", Array(uid, oid, iid, amt), Array(uid))
    val items = Table("itmTT3", Array(iid, desc), Array(iid))
    
    val (udb, odb, idb) = (new DBMgr(users), new DBMgr(orders), new DBMgr(items))
    val link1 = Link(Array(uid), users, FkRule(Restrict, Cascade))
    odb.addForeignKey(link1) // added to table containing foreign key, not primary key
    val link2 = Link(Array(iid), items, FkRule(Restrict, Cascade))
    odb.addForeignKey(link2)
    udb.insert(Array("user1", "user@someplace.com", BigInt(922337)))
    udb.insert(Array("user2", "user2@hotmail2.com", 34344))
    idb.insert(Array("item1", "item1Desc"))
//    udb.selectStar(uid, email, bal).as(arrayToPerson) foreach println
    
    odb.insert("user1", "order1", "item1", "amt")
    
  }
}
