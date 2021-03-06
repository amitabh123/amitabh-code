
package test

import CommonTest._
import mux.db.config.TraitDBConfig
import mux.db.core.DataStructures._
import mux.db.core._
import mux.db.BetterDB._
import mux.db.DBManager

object Maint extends App{
  case class Person(uid:String, email:String, value:BigInt, scan:Array[Byte])
  def arrayToPerson(a:Array[Any]) = {
    Person(a(0).asInstanceOf[String], a(1).asInstanceOf[String], a(2).asInstanceOf[BigInt], a(3).asInstanceOf[Array[Byte]])
  }
  
  // copies from old to new
  object OldConfig extends TraitDBConfig {
    val dbname:String = "mydbOld"
    val dbhost:String = "localhost"
    val dbms:String = "h2"  // e.g. mysql or h2 or postgresql
    val dbuser:String = "aliceOld"
    val dbpass:String = "abcdef"
    val connTimeOut:Int = 1000 // seconds
    val usePool:Boolean = true // e.g. true (use db pool)
    val configSource = "test"
  }
  
  object NewConfig extends TraitDBConfig {
    val dbname:String = "mydbNew"
    val dbhost:String = "localhost"
    val dbms:String = "h2"  // e.g. mysql or h2 or postgresql
    val dbuser:String = "aliceNew"
    val dbpass:String = "xyzw"
    val connTimeOut:Int = 1000 // seconds
    val usePool:Boolean = true // e.g. true (use db pool)
    val configSource = "test"
  } 
  foo
  
  def foo = usingExit { // exit on done
 
    val (uid, email, value, scan) = (Col("uid", VARCHAR(255)), Col("email", VARCHAR(255)), Col("value", UScalaBIGINT(100)), 
                                     Col("passportScan", BLOB))
    val tableOld = Table("blobsOldTable", Array(uid, email, value, scan), Array(uid))
    val table = Table("blobsNewTable", Array(uid, email, value, scan), Array(uid))
    val dbmOld = new DBManager(tableOld)(NewConfig)
    val dbmNew = new DBManager(table)(NewConfig)
    import amit.common.Util._
    dbmOld.insert(Array("hey"+randomAlphanumericString(10), "hello", BigInt(9223372036854775806L), Array[Byte](33, 43)))
    println (" dmbOld : read following ")
    dbmOld.select(uid, email, value, scan).as(arrayToPerson) foreach println
    dbmNew.deleteAll
    println (" dmbNew : read following before import")
    dbmNew.select(uid, email, value, scan).as(arrayToPerson) foreach println
    val testFile = "test12345"+randomAlphanumericString(5)+".txt"
    amit.common.file.Util.deleteFile(testFile)
    dbmOld.exportToCSV(testFile, Array())
    dbmNew.importFromCSV(testFile)
    dbmNew.select(uid, email, value, scan).as(arrayToPerson) foreach println
  }
}
