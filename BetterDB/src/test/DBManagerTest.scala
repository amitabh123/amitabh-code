
package test

import CommonTest._
import mux.db.core.DataStructures._
import mux.db.core._
import mux.db._
import BetterDB._

object DBManagerTest {
  
  def main(args:Array[String]):Unit = {}
//  
  val STR = VARCHAR(255)
  val name = Col("name", STR)
  val age = Col("age", UINT)
  val sal = Col("sal", UINT)
  val user = Table("user", Array(name, age, sal), Array[Col]())
  
  usingExit {
    val db = new DBManager(user)(MyDBConfig)
    if (db.isEmpty) {
      db.insert(Array("alice", 20, 25000))
      db.insert(Array("arun", 25, 20000))
      db.insert(Array("amit", 30, 30000))
      db.insert(Array("ajit", 19, 10000))
      db.insert(Array("max", 59, 50000))
      db.insert(Array("alice", 28, 18000))
      db.insert(Array("bob", 43, 48000))
    }
    db.aggregate(age.min, age.last, age.first, age.top).where(age <= 100, age >= 10).groupByInterval(age \ 10).asInt.map{x => 
      "minAge:"+x(0)+", last:"+x(1)+"; first:"+x(2)+"; base:"+x(3)
    }.foreach(println)
  /*     
  // results in query
  SELECT MIN(user.age) as bKyUMKnNBrMIN,GROUP_CONCAT(user.age) as ayyaVzhJYUGROUP_CONCAT,GROUP_CONCAT(user.age) as zCbCzfkYGkGROUP_CONCAT,10*round(user.age/10.0, 0) as azFqJaJkAbinterval10 FROM user WHERE user.age <= 100 and user.age >= 10 GROUP BY azFqJaJkAbinterval10
   */ 
  /*     
  // prints
  minAge:19, last:19; first:20; base:20
  minAge:59, last:59; first:59; base:60
  minAge:43, last:43; first:43; base:40
  minAge:25, last:28; first:25; base:30    
   */ 
  } 
}