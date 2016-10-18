
package mux.db

import mux.db.core.DataStructures._
import mux.db.core.FKDataStructures.Action
import mux.db.core.FKDataStructures.Cascade
import mux.db.core.FKDataStructures.FkRule
import mux.db.core.FKDataStructures.Link
import mux.db.core.FKDataStructures.Restrict
import mux.db.core.Table
import mux.db.{DBManager => DBM}

object BetterDB {
  object Implicits {
    implicit def strToCol(a:String) = constCol(a)
    implicit def intToCol(a:Int) = constCol(a)
    implicit def longToCol(a:Long) = constCol(a)
    implicit def bigIntToCol(a:BigInt) = constCol(a)
  }
  type Max = Int
  type Offset = Long
  type toT[T] = Array[Any] => T
  type DBSelect[T] = (DBM, Cols, Wheres, Max, Offset, Orderings, toT[T])
  type DBAggr = (DBM, Aggregates, Wheres, Havings)
  type DBAggrGrp = (DBM, Aggregates, Wheres, Havings, GroupByIntervals)
  
  
  case class Sel[T](qry:DBSelect[T]){
    val (db, origCols, origWheres, origMax, origOffset, origOrderings, origToT) = qry
    def execute = db.selectCols(origWheres, origCols, origToT)(origOrderings, origMax,origOffset)
    def where(wheres:Wheres) = Sel(db, origCols, origWheres ++ (wheres.filterNot(origWheres.contains)), origMax, origOffset, origOrderings, origToT)
    def where(wheres:Where*):Sel[_] = where(wheres.toArray)// Qry(db, origCols, origWheres ++ (wheres.filterNot(origWheres.contains)), origMax, origOffset, origOrderings, origToT)
    def select(cols:Cols) = Sel(db, origCols ++ (cols.filterNot(origCols.contains)), origWheres, origMax, origOffset, origOrderings, origToT)
    def select(cols:Col*):Sel[_] = select(cols.toArray)
    def max(max:Int) = Sel(db, origCols, origWheres, max, origOffset, origOrderings, origToT)
    def offset(offset:Long) = Sel(db, origCols, origWheres, origMax, offset, origOrderings, origToT)
    def orderBy(orderings:Orderings) = Sel(db, origCols, origWheres, origMax, origOffset, origOrderings ++ (orderings.filterNot(origOrderings.contains)), origToT)
    def orderBy(orderings:Ordering*):Sel[_] = orderBy(orderings.toArray) // Qry(db, origCols, origWheres, origMax, origOffset, origOrderings ++ (orderBy.filterNot(origOrderings.contains)), origToT)
    //////
    def as[B](arrayAnyToT:toT[B]) = Sel(db, origCols, origWheres, origMax, origOffset, origOrderings, arrayAnyToT).execute
    def castFirstAs[T] = as(_(0).as[T])
    def firstAs[T](anyToT:Any => T) = as(a => anyToT(a(0)))
    def asList = Sel[List[Any]](db, origCols, origWheres, origMax, origOffset, origOrderings, _.toList).execute
    def into(otherDB:DBM) = db.selectInto(otherDB, origWheres, origCols)(origOrderings, origMax,origOffset)
    @deprecated("Nested are resource intensive", "2 June 2016") def nested = { // possible problem if origCol has tableSpecificInfo
      if (origCols.size != 1) throw DBException("BetterDB: selected cols size must be 1 in nested select")
      val table = db.getTable
      
      val ords = origOrderings.map(_.to(table))
      NestedSelect(db, origCols(0).to(table), origWheres.map(_.to(table)), origOrderings.map(_.to(table)), origMax, origOffset)
    }
  }

  case class Agg(qry:DBAggr) {
    val (db, origAggrs, origWheres, origHavings) = qry
    def where(wheres:Wheres) = Agg(db, origAggrs, origWheres ++ (wheres.filterNot(origWheres.contains)), origHavings)
    def where(wheres:Where*):Agg = where(wheres.toArray)
    def aggregate(aggregates:Aggregates) = Agg(db, origAggrs ++ (aggregates.filterNot(origAggrs.contains)), origWheres, origHavings)
    def aggregate(aggregates:Aggregate*):Agg = aggregate(aggregates.toArray)    
    def firstAsLong = asLong(0).as[Long]
    def firstAsInt = asInt(0).as[Int]
    def firstAsDouble = asDouble(0).as[Double]
    def firstAsBigInt = asBigInt(0).as[BigInt]
    def having(havings:Havings) = Agg(db, origAggrs, origWheres, origHavings ++ (havings.filterNot(origHavings.contains)))
    def having(havings:Having*):Agg = having(havings.toArray)
    
    def asLong = db.aggregateLong(origAggrs, origWheres, origHavings) 
    def asInt = db.aggregateInt(origAggrs, origWheres, origHavings)
    def asDouble = db.aggregateDouble(origAggrs, origWheres, origHavings)
    def asBigInt = db.aggregateBigInt(origAggrs, origWheres, origHavings)
    
    def groupByInterval(groupByIntervals:GroupByIntervals) = Grp(db, origAggrs, origWheres, origHavings, groupByIntervals)
    def groupByInterval(groupByIntervals:GroupByInterval*):Grp = groupByInterval(groupByIntervals.toArray)
    def groupBy(cols:Cols) = groupByInterval(cols.map(_ \ 0))
    def groupBy(cols:Col*):Grp = groupBy(cols.toArray)
    def nested = { // possible problem if origCol has tableSpecificInfo
      if (origAggrs.size != 1) throw DBException("BetterDB: selected aggregates size must be 1 in nested aggregate")
      NestedAggregate(db, origAggrs(0).to(db.getTable), origWheres, Array(), origHavings)
    }
  }
  case class Grp(qry:DBAggrGrp) {
    val (db, origAggrs, origWheres, origHavings, origGrps) = qry
    def where(wheres:Wheres) = Grp(db, origAggrs, origWheres ++ (wheres.filterNot(origWheres.contains)), origHavings, origGrps)
    def where(wheres:Where*):Grp = where(wheres.toArray)
    def aggregate(aggregates:Aggregates) = Grp(db, origAggrs ++ (aggregates.filterNot(origAggrs.contains)), origWheres, origHavings, origGrps)
    def aggregate(aggregates:Aggregate*):Grp = aggregate(aggregates.toArray)    
    def groupByInterval(groupByIntervals:GroupByIntervals) = Grp(db, origAggrs, origWheres, origHavings, origGrps ++ (groupByIntervals.filterNot(origGrps.contains)))
    def groupByInterval(groupByIntervals:GroupByInterval*):Grp = groupByInterval(groupByIntervals.toArray)
    def firstAsLong = asLong.map(_(0).as[Long])
    def firstAsInt = asInt.map(_(0).as[Int])
    def firstAsDouble = asDouble.map(_(0).as[Double])
    def firstAsBigInt = asBigInt.map(_(0).as[BigInt])
    def having(havings:Havings) = Grp(db, origAggrs, origWheres, origHavings ++ (havings.filterNot(origHavings.contains)), origGrps)
    def having(havings:Having*):Grp = having(havings.toArray)
    def asLong = db.aggregateGroupLong(origAggrs, origWheres, origGrps, origHavings) 
    def asInt = db.aggregateGroupInt(origAggrs, origWheres, origGrps, origHavings)
    def asDouble = db.aggregateGroupDouble(origAggrs, origWheres, origGrps, origHavings)
    def asBigInt = db.aggregateGroupBigInt(origAggrs, origWheres, origGrps, origHavings)    
    def groupBy(cols:Cols) = groupByInterval(cols.map(_ \ 0))
    def groupBy(cols:Col*):Grp = groupBy(cols.toArray)
    def nested = { // possible problem if origCol has tableSpecificInfo
      if (origAggrs.size != 1) throw DBException("BetterDB: selected aggregates size must be 1 in nested aggregate")
      NestedAggregate(db, origAggrs(0).to(db.getTable), origWheres, origGrps, origHavings)
    }
  }
  type DBIncr = (DBM, Increments, Wheres)
  case class Inc(qry:DBIncr) {
    val (db, origIncrs, origWheres) = qry
    def execute = db.incrementColsTx(origWheres, origIncrs)
    def increment(increments:Increments) = Inc(db, origIncrs ++ (increments.filterNot(origIncrs.contains)), origWheres)
    def increment(increments:Increment*):Inc = increment(increments.toArray)
    def where(wheres:Wheres) = Inc(db, origIncrs, origWheres ++ (wheres.filterNot(origWheres.contains))).execute
    def where(wheres:Where*):Int = where(wheres.toArray)
  }
  type DBUpd = (DBM, Updates[Any], Wheres)
  case class Upd(qry:DBUpd) {
    val (db, origUpds, origWheres) = qry
    def execute = db.updateCols(origWheres, origUpds)
    def update(updates:Updates[Any]) = Upd(db, origUpds ++ (updates.filterNot(origUpds.contains)), origWheres)
    def update(updates:Update[Any]*):Upd = update(updates.toArray)
    def where(wheres:Wheres) = Upd(db, origUpds, origWheres ++ (wheres.filterNot(origWheres.contains))).execute
    def where(wheres:Where*):Int = where(wheres.toArray)
  }
  class BetterDB(val db:DBM) {
    def select(cols:Col*):Sel[_] = select(cols.toArray) 
    def select(cols:Cols) = Sel(db, cols, Array(), Int.MaxValue, 0, Array(), (a:Array[Any]) => a.toList)
    def selectStar = Sel(db, db.getTable.tableCols, Array(), Int.MaxValue, 0, Array(), (a:Array[Any]) => a.toList)
    def aggregate(aggrs:Aggregate*) = Agg(db, aggrs.toArray, Array(), Array())
    def aggregate(aggrs:Aggregates):Agg = aggregate(aggrs:_*)
    def deleteWhere(wheres:Where*) = db.delete(wheres.toArray)
    def countWhere(wheres:Where*) = db.countRows(wheres.toArray)
    def increment(increments:Increment*) = Inc(db, increments.toArray, Array())
    def update(updates:Update[Any]*):Upd = update(updates.toArray)
    def update(updates:Updates[Any]) = Upd(db, updates, Array())
    def insert(anys:Any*) = {
      // what happens to Array[Byte] for Blob type ?
      anys.size match {
        case 1 =>
          anys(0) match {
            case _:Array[Byte] => db.insertArray(anys.toArray)
            case a:Array[_] => db.insertArray(a.asInstanceOf[Array[Any]])
            case _ => db.insertArray(anys.toArray)
          }
        case _ => db.insertArray(anys.toArray)
      }
    }
    def addForeignKey(cols:Cols) = FK(cols, None, None, None, db.addForeignKey)
    def addForeignKey(cols:Col*):FK = addForeignKey(cols.toArray)
    def removeForeignKey(cols:Cols) = FK(cols, None, None, None, db.removeForeignKey)
    def removeForeignKey(cols:Col*):FK = removeForeignKey(cols.toArray)
  }
  case class FK(cols:Cols, optPriKeyTable:Option[Table], optOnDelete:Option[Action], optOnUpdate:Option[Action], doOperation:Link => Int){
    def toPriKeyOf(priKeyDB:DBM):FK = toPriKeyOf(priKeyDB.getTable)
    def toPriKeyOf(priKeyTable:Table) =  FK(cols, Some(priKeyTable), optOnDelete, optOnUpdate, doOperation)
    def onDeleteRestrict = FK(cols, optPriKeyTable, Some(Restrict), optOnUpdate, doOperation)
    def onUpdateRestrict = FK(cols, optPriKeyTable, optOnDelete, Some(Restrict), doOperation)
    def onDeleteCascade = FK(cols, optPriKeyTable, Some(Cascade), optOnUpdate, doOperation)
    def onUpdateCascade = FK(cols, optPriKeyTable, optOnDelete, Some(Cascade), doOperation)
    if (optPriKeyTable.isDefined && optOnDelete.isDefined && optOnUpdate.isDefined)
      doOperation(Link(cols, optPriKeyTable.get, FkRule(optOnDelete.get, optOnUpdate.get)))
  }
  implicit def dbToDB(s: DBM) = new BetterDB(s)
  implicit def selToNested(s:Sel[_]) = s.nested

}
///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////