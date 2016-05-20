
package mux.db

import mux.db.core.FKDataStructures._
import mux.db.core._
import mux.db.core.Util._
import mux.db.config.TraitDBConfig
import mux.db.core.DataStructures._
import amit.common.Util._

abstract class DBManagerDDL(table:Table, dbConfig:TraitDBConfig) extends DBManagerDML(table:Table, dbConfig:TraitDBConfig) {  
  import table._
  // FOREIGN KEY MANAGEMENT
  def removeIndexIfExists(cols:Col*) = {
    using(connection) {
      conn => using(conn.prepareStatement(dropIndexString(cols.toArray))){_.executeUpdate}
    }
  }
  def indexBy(cols:Col*) = {
    cols.foreach(c => assert(c.optTable.isEmpty, "indexCols should not refer to tables"))
    cols.foreach(c => assert(!c.isComposite, "indexCols cannot be composite"))
    cols.foreach(assertColExists)  
    using(connection) { 
      conn => using(conn.prepareStatement(createIndexString(cols.toArray))){_.executeUpdate}
    } 
  }
  //  //  unused below (commented out)
  //  def isIndexExists(cols:Cols) = using(connection){ conn =>
  //    using(conn.prepareStatement(getSelectIndexStr(cols))) { st =>
  //      using(st.executeQuery) { _.next }
  //    }
  //  }
  
  private def isFKLinkExists(link:Link) = using(connection){ conn =>
    val sql = if (dbConfig.dbms == "postgresql") getFKConstraintStrPostgreSQL(link) else getFKConstraintStr(link)
    using(conn.prepareStatement(sql)) { st =>
      using(st.executeQuery) { _.next }
    }
  }

  def addForeignKey(link:Link) = {
    link.fkCols foreach assertColExists
    if (table == link.pkTable && !link.pkCols.intersect(link.fkCols).isEmpty) // referring to same column in same table.. not allowed
      throw new DBException("Foreign Key: cols in table["+table+"]: cannot refer to itself.")
    if (!isFKLinkExists(link)) using(connection) { 
      conn => using(conn.prepareStatement(createFkLinkString(link))){_.executeUpdate}
    } else 0
  }
  def removeForeignKey(link:Link) = {
    link.fkCols foreach assertColExists
    if (table == link.pkTable && !link.pkCols.intersect(link.fkCols).isEmpty) // referring to same column in same table.. not allowed
      throw new DBException("Foreign Key: cols in table["+table+"]: cannot refer to itself.")
    if (isFKLinkExists(link)) using(connection) { 
      conn => using(conn.prepareStatement(dropFkLinkString(link))){_.executeUpdate}
    } else 0
  }
}
