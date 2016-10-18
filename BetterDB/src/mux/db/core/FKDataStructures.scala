package mux.db.core

import mux.db.DBManager
import mux.db.core.DataStructures._
import mux.db.core._

object FKDataStructures {
  /*
   * Restrictions:
   * 
   * Composite primary keys cannot be used. A foreign key must be a single column and refer to a table
   * with the same column type as the primary key. That table should have exactly one primary key
   */
  abstract class Action {override def toString:String}
  
  object Cascade extends Action {override def toString = "CASCADE"}
  object Restrict extends Action {override def toString = "RESTRICT"}
  
  case class FkRule(onDelete:Action, onUpdate:Action) 
  object Link {
    def apply(fkCol:Col, pkTable:Table, rule:FkRule) = new Link(fkCol:Col, pkTable:Table, rule:FkRule)      
    def apply(fkCol:Col, pkDB:DBManager, rule:FkRule) = new Link(fkCol, pkDB.getTable, rule)
    def apply(fkCols:Cols, pkDB:DBManager, rule:FkRule) = new Link(fkCols, pkDB.getTable, rule)
  }
  case class Link(fkCols:Cols, pkTable:Table, rule:FkRule) {
    def this(fkCol:Col, pkTable:Table, rule:FkRule) = this(Array(fkCol), pkTable, rule)
    val pkCols = pkTable.priKey
    if (fkCols.toSet.size != pkCols.size) throw new DBException("foreign key cols contain duplicates")
    if (fkCols.size != pkCols.size) 
      throw new DBException("table["+pkTable+"] primary key cardinalty["+pkCols.size+"]"+
                            " does not match foreign key cardinality["+fkCols.size+"]")
    (fkCols.zip(pkCols)) foreach {
      case (fkCol, pkCol) if fkCol.colType != pkCol.colType=> 
        throw new DBException("table["+pkTable+"] primary key col["+pkCol+"]'s type["+pkCol.colType+"] does not match "+
                              "foreign key col["+fkCol+"]'s type["+pkCol.colType+"]")
      case _ => 
    }
  }
}


//  case class Link(fkCol:Col, pkTable:Table, rule:FkRule) { // later on make fkCol as an Array of Col to handle compisite keys
//    if (pkTable.priKey.size != 1) 
//      throw new DBException("table["+pkTable+"] composite primary keys not allowed. Use LinkComposite.")
//    val pkCol = pkTable.priKey.head
//    if (fkCol.colType != pkCol.colType) 
//      throw new DBException("FK Constraint: PK col["+pkTable+"]:["+pkCol+"]'s type["+pkCol.colType+"] "+
//                            "does not match FK col["+fkCol+"]'s type["+fkCol.colType+"]" )
//  }

