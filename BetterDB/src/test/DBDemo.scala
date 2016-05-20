
package test

import CommonTest._
import mux.db.core._
import mux.db.{DBManager => DBMgr}
import amit.common.json.JSONUtil.JsonFormatted
import mux.db.BetterDB._
import mux.db.core.DataStructures._
import mux.db.core.DataStructures.{VARCHAR => STR}

object SameTableKey extends App {
  val key1 = Col("ID1", STR(255))
  val key2 = Col("ID2", STR(255))
  val db = DBMgr("samekey_test")(key1, key2)(key1, key2)(MyDBConfig)
//  db
  db.insert("1", "2")
  db.insert("2", "3")
  db.insert("3", "4")
  db.insert("4", "5")
  db.insert("7", "9")
  db.selectStar.execute foreach println
//  db.selectStar.execute foreach println
  db.select(key1.of(db), key2.of(db)).where((key1.of(db), Eq, key2.of(db))).asList foreach println
  System.exit(1)
}

object DBDemo extends App
{
  val userIDCol = Col("userID", STR(255))
  val ageCol = Col("age", INT)
  val salaryCol = Col("salary", LONG)
  val passportScanCol = Col("passport", BLOB)
  
  val userTableCols = Array(userIDCol, ageCol, salaryCol, passportScanCol)
  val userTablePriKeys = Array(userIDCol)
  
  val userTable = Table("userTable", userTableCols, userTablePriKeys)
  implicit val config = MyDBConfig
  val userDBMgr = new DBMgr(userTable) // using implicit config
  // val userDBMgr = new DBMgr(userTable)(MyDBConfig)  // using explicit config
  // table will be created automatically if not exists
  
  // define data to be inserted
  val userID = "Amit"
  val age = 20
  val salary = 1000000000L
  val passportScan:Array[Byte] = Array(1, 2, 3)
  usingExit {    
    // insert data into table
    userDBMgr.insert(userID, age, salary, passportScan)

    // define a case class to hold user details
    case class User(userID:String, salary:Long, passportScan:Array[Byte]) extends JsonFormatted {
      val keys = Array("userID", "salary", "passportScan")
      val vals = Array(userID, salary, passportScan.toList)
    }

    // define a method to convert Array[Any] to User
    def toUser(a:Array[Any]) = {
          User(a(0).asInstanceOf[String], a(1).asInstanceOf[Long], a(2).asInstanceOf[Array[Byte]])
    }
    // search db 
    val users:List[User] = userDBMgr.select(userIDCol, salaryCol, passportScanCol).where(userIDCol === "Amit" and ageCol < 25).as(toUser)

    userDBMgr.select(userIDCol, salaryCol, passportScanCol).where(userIDCol === "Amit" and ageCol < 25).as(toUser) foreach println // applies toUser. Returns each row as User
    userDBMgr.select(userIDCol, salaryCol, passportScanCol).where(userIDCol === "Amit" and ageCol < 25).execute foreach println // returns each row as List[Any]
    userDBMgr.select(userIDCol, salaryCol, passportScanCol).where((userIDCol.value = "Amit") and ageCol < 25).execute foreach println // same as above
    // above two are equivalent userIDCol.value = "Amit" and userIDCol === "Amit" are identical. 
  } 
}
