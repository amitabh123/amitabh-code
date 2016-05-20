
package test

import CommonTest._
import mux.db.core.DataStructures._
import mux.db.core._
import mux.db.BetterDB
import mux.db.DBManager
import mux.db.{DBManager => DBMgr}
import BetterDB._
import BetterDB.Implicits._

object AllTest extends App {
  val STR = VARCHAR(255)
  val name = Col("NAME", STR)
  val age = Col("AGE", UINT)
  val sal = Col("SAL", UINT)
  val user = Table("USERS", name, age, sal)
  val db = new DBMgr(user)(MyDBConfig)
  case class User(name:String, age:Int, sal:Int)
  object User{def apply(a:Array[Any]) = new User(a(0).as[String], a(1).as[Int], a(2).as[Int])}
  def asUser(a:Array[Any]) = User(a)
  aggregateExample
  def insertExample{
    // insert some data
    db.insert("alice", 20, 25000)
    db.insert("bob", 25, 20000)
    db.insert("carol", 30, 30000)
    db.insert("james", 19, 10000)
    db.insert("max", 59, 50000)
    db.insert("alice", 28, 18000)
    db.insert(Array("bob", 43, 48000))    // can use Array 
    db.insert(List("bob", 43, 48000))    // can use List
  }
  
  def colOperationExamples { 
    val a = Col("a", INT) // define a column
    val b = Col("b", INT) // define a column
    a + (b + 1) // composite col, also of type Col
    Col("2", INT) === "3" // returns Where(Col("2", INT), Eq, "3")
    Col("2", INT) <> "3" // returns Where(Col("2", INT), Ne, "3")
    Col("3", INT) like "4"  // returns Where(Col("2", INT), Like, "3")
  }
  def indexExample {
    usingExit {    // exit after finishing or on error
      // index examples
      db.indexBy(sal+name) // create index // should give error
      db.indexBy(sal, age) // create index
      db.removeIndexIfExists(sal, age) // remove index
    }
  }
  def aggregateExample {
    usingExit {    // exit after finishing or on error
      insertExample
      // SQL query disection
      // below defines an mapping for AGGREGATE SQL query.        
      /////////////////////////////////////
      //  Query
      /////////////////////////////////////
      db.aggregate( // aggregate
        (sal/2).sum / (age*sal).avg,  // composite aggregate (composed of / + - etc)
        sal.avg, // simple aggregate
        sal.sum / sal.avg, // composite aggregate
        sal.count, // simple aggregate
        age \ 3, // group-by-interval (with interval 3). Does not require a groupBy clause. NOTE: age \ 3 is implictly translated to Aggregate(this, Top(Some(interval))) as explained below
        (sal / 2), // column, requires a groupBy clause for (sal / 2). NOTE: (sal / 2) is implictly translated to Aggregate((sal / 2), GroupBy)
        name // column, requires a groupBy clause for name. NOTE: name is implictly translated to Aggregate(name, GroupBy)
      ).where(
        age <= 100 and age >= 10 // where clauses
      ).groupByInterval(
        (sal / 2) \ 40, 
        age \ 3
      ).groupBy(
        (sal / 2),
        name
      ).asLong.map(
        _.toList
      ).foreach(println)

      
      /////////////////////////////////////
      //  SQL WITHOUT DATA
      /////////////////////////////////////
      /*  
      SELECT 
            (SUM((USERS6Am9.SAL / ?)) / AVG((USERS6Am9.AGE * USERS6Am9.SAL))) as YXRadfVfaCcomposite,
            AVG(USERS6Am9.SAL) as ehDBWZhZuyAVG,
            (SUM(USERS6Am9.SAL) / AVG(USERS6Am9.SAL)) as JkHVFaNNlLcomposite,
            COUNT(USERS6Am9.SAL) as rQNYoFnxGMCOUNT,
            40*round((USERS6Am9.SAL / ?)/40.0, 0) as wolerVOASVinterval40,
            3*round(USERS6Am9.AGE/3.0, 0) as UFKJUFXLSuinterval3,
            (USERS6Am9.SAL / ?) as wolerVOASVinterval0,
            USERS6Am9.NAME as WPJTPsALSrinterval0 
        FROM USERS6Am9 
        WHERE 
           (USERS6Am9.AGE <= ? 
                and 
            USERS6Am9.AGE >= ?) 
        GROUP BY 
            wolerVOASVinterval40,
            UFKJUFXLSuinterval3,
            wolerVOASVinterval0,
            WPJTPsALSrinterval0
      */ 
     
      /////////////////////////////////////
      //  SQL WITH DATA
      /////////////////////////////////////
      /*
      SELECT 
            (SUM((USERS6Am9.SAL / 2)) / AVG((USERS6Am9.AGE * USERS6Am9.SAL))) as YXRadfVfaCcomposite,
            AVG(USERS6Am9.SAL) as ehDBWZhZuyAVG,
            (SUM(USERS6Am9.SAL) / AVG(USERS6Am9.SAL)) as JkHVFaNNlLcomposite,
            COUNT(USERS6Am9.SAL) as rQNYoFnxGMCOUNT,
            40*round((USERS6Am9.SAL / 2)/40.0, 0) as wolerVOASVinterval40,
            3*round(USERS6Am9.AGE/3.0, 0) as UFKJUFXLSuinterval3,
            (USERS6Am9.SAL / 2) as wolerVOASVinterval0,
            USERS6Am9.NAME as WPJTPsALSrinterval0 
        FROM USERS6Am9 
        WHERE 
           (USERS6Am9.AGE <= 100 
                and 
            USERS6Am9.AGE >= 10) 
        GROUP BY 
            wolerVOASVinterval40,
            UFKJUFXLSuinterval3,
            wolerVOASVinterval0,
            WPJTPsALSrinterval0       
      */

      // // Examples of implicit conversions below:
      val grpByInterval = age \ 3 // returns GroupByInterval(age, 3) due to 1. below
      val top:Aggregate = age \ 3 // returns Aggregate(age, Top(Some(3)) via implicit conversion
      //  // 1. (Col.scala)  def \(interval:Long)    = GroupByInterval(this, interval) // returns GroupByInterval
      //  // 2. (package.scala)  implicit def groupByIntervalToTop(groupByInterval:GroupByInterval) = 
      //  //        Aggregate(groupByInterval.col, Top(Some(groupByInterval.interval)))  // returns Aggregate(age, Top(Some(3))
      
      name === 3 // returns Where(name, Eq, 3)

      // more aggregate examples below
      db.aggregate(age.max, sal.max).where(sal < 30000 and (age > 20 or sal > 20000)).groupBy(sal / 10000).asDouble.foreach(println)
      db.aggregate(age.max, sal.max).where(sal < 30000 and (age > 20 or sal > 20000)).groupBy((sal*age)/10).asDouble.foreach(println)
      db.aggregate(age.max, sal.max).where(sal < 30000 and (age > 20 or sal > 20000)).groupBy((sal*age), sal / 334).asDouble.foreach(println)
      db.aggregate(age.max, sal.max).where(sal < 30000 and (age > 20 or sal > 20000)).asDouble.foreach(println)
      // below uses having clause
      db.aggregate(sal.max).where(sal <> 10001).having(age.max/age.min <> 100).asLong foreach println

    }
    def updateExample{
      // update rows
      db.update(age -> 54 , sal -> 434).where(age === 43 and sal < 30000) // uses implicit -> from Scala PreDef to return (A, B), then uses implicit conversion to Update
      db.update(
        age -> 54, // -> is Scala's predef 
        sal -> 434
      ).where((age.value = 45) and (age === 45))
      db.update(age <-- 54, // uses <-- defined in Col
                sal -> 434).where((age.value = 45) and (age === 45))
      db.update(age -> 54 , sal -> 434).where((age.value = "a"))
      db.update(name <-- "Bob")
    }
    def incrementExample {
      // increment some cols
      db.increment(age ++= 54, sal ++= 45).where(age <> 43 and sal < 30000)
    } 
  }

  
  def nestedSelectExample { 
    val sel = db.select(age).where(sal > 4000).orderBy(sal.decreasing).max(10).offset(10)
    sel.execute foreach println
    
    val x = sal from sel // implicit conversion to nested... Explicit shown below
    
    val agg = db.aggregate(sal.max).where(sal > 10000).having(age.max < 100).nested
    agg.debugExecute foreach println
    
    db.select(age).where(sal from agg).execute foreach println
    db.select(age).where(sal from sel).execute foreach println
    db.select(age).where(sal in agg).execute foreach println
    db.select(age).where(sal in sel).execute foreach println
    db.select(age).where(sal notIn agg).execute foreach println
    db.select(age).execute foreach println
  }
  def selectExample {
    val sel = db.select(age).where(sal > 4000).orderBy(sal.decreasing).max(10).offset(10)
    db.select(
      (age.of(db) -10) + (sal.of(db) - 60000) * age.of(db), 
      age,constCol("he"),
      (34 / 5),
      34 / age,
      sal
    ).where( 
      sal / 1000 >= age - 10, 
      sal / 1000 >= age - 4 and 
      (sal / 1234 >= age - 6) or
      (sal from sel)
    ).orderBy(
      (age+sal+2, Decreasing),
      age.decreasing,
      sal
    ).max(
      10
    ).offset(
      2
    ).asList.foreach(println)
  }
  def moreAggregateExamples = {    
    db.aggregate(sal.last, (sal / 2).top).where(age <= 100 and age >= 10).groupByInterval((sal / 2) \ 40, age \ 40).asLong.map(_.toList).foreach(println)
    db.aggregate(sal.last, age.avg, sal \ 10).groupByInterval(sal \ 10).asLong.map(_.toList).foreach(println)
    db.aggregate(
      (sal/2).sum / (age*sal).avg, 
      sal.avg, 
      sal.sum / sal.avg, 
      sal.count, 
      (sal / 2) \ 40
    ).where(
      age <= 100 and age >= 10
    ).groupByInterval(
      (sal / 2) \ 40, // with interval 40, will be used as a group-by-interval
      age \ 3, // with interval 3, will be used as a group-by-interval
      age \ 0 // with interval 0, will be used as a group-by
    ).asLong.map(
      _.toList
    ).foreach(println)

    db.aggregate(
      (sal/2).sum / (age*sal).avg, 
      sal.avg, 
      sal.sum / sal.avg, 
      sal.count,
      sal,
      age \ 3,
      (sal / 2)
    ).where(
      age <= 100 and age >= 10
    ).groupByInterval(
      (sal / 2) \ 40, 
      age \ 3
    ).groupBy(
      (sal / 2),
      name,
      sal
    ).asLong.map(
      _.toList
    ).foreach(println)

    db.aggregate(
      (sal/2).sum / (age*sal).avg, 
      sal.avg, 
      sal.sum / sal.avg, 
      sal.count, 
      (sal / 2) \ 40 // maps to Top(40) ... should give an error (no group-by-interval clause present and Top used)
    ).where(
      age <= 100 and age >= 10
    ).asLong.foreach(println)
    
    db.aggregate(
      (sal/2).sum * 1000 / (age*sal).avg, 
      sal.avg, 
      sal.sum / sal.avg, 
      sal.count
    ).where(
      age <= 100 and age >= 10
    ).asLong.foreach(println)
  }
}
object Main extends App // more examples
{
  case class Person(uid:String, email:String, value:BigInt, scan:Array[Byte])
  def arrayToPerson(a:Array[Any]) = {
    Person(a(0).asInstanceOf[String], a(1).asInstanceOf[String], a(2).asInstanceOf[BigInt], a(3).asInstanceOf[Array[Byte]])
  }
  foo
  def foo = usingExit {
    val (uid, email, value, scan) = (Col("uid", VARCHAR(255)), Col("email", VARCHAR(255)), Col("value", UScalaBIGINT(100)), 
                                     Col("scan", BLOB))
    val table = Table("blobss", Array(uid, email, value, scan), Array(uid))
    val dbm = new DBManager(table)(MyDBConfig)
    
    println(" Create string: "+dbm.getTable.createSQLString)
    
    dbm.deleteAll
    dbm.deleteWhere() // equal to above
    
    dbm.insert(Array("alice", "alice@mail.com", BigInt(9223372036854775806L), Array[Byte](33, 43)))
    dbm.insert(Array("bob", "bob@gmail.com", 1l, Array[Byte](11, 21)))
    dbm.insert("carol", "no_email", 51222, Array[Byte](16, 32))
    
    // read all data
    dbm.select(uid,email, value, scan).as(arrayToPerson) foreach println

    dbm.incrementColTx(Where(uid, Eq, "alice"), Update(value, -3372058))
    dbm.increment(value ++= -3372058).where(uid === "alice") // same as above

    dbm.select(uid, email, value, scan).as(arrayToPerson) foreach println
    dbm.incrementColsTx(Array(Where(uid, Eq, "alice")), Array(Update(value, 33)))
    dbm.select(uid,email, value, scan).as(arrayToPerson) foreach println
    dbm.incrementColsTx(Array(Where(uid, Eq, "bob")), Array(Update(value, BigInt("-32020029223372058")))) // should give error
    dbm.select(uid, email, value, scan).as(arrayToPerson) foreach println
    dbm.select(uid, email, value, scan).as(arrayToPerson) foreach println
  } 
}

////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////












