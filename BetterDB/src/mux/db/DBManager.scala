package mux.db

import mux.db.config._
import mux.db.core.Util._
import mux.db.core._
import mux.db.core.DataStructures._
import amit.common.Util._
import amit.common.json.JSONUtil.JsonFormatted
import java.io.File
import java.sql.ResultSet
import java.sql.{ResultSet => RS}
import mux.db.core.FKDataStructures._
import DBManagerMgmt._

object DBManager {
  val noCols = Array[Col]()  
  def apply(name:String)(cols:Col *)(priCols:Col *)(implicit config:TraitDBConfig = DefaultDBConfigFromFile) = new DBManager(Table(name, cols.toArray, priCols.toArray))(config)
  def apply(name:String, cols:Cols, priCols:Cols)(implicit config:TraitDBConfig):DBManager = apply(name)(cols: _ *)(priCols: _ *)(config)
  def apply(name:String, cols:Cols, priCol:Col)(implicit config:TraitDBConfig):DBManager = apply(name, cols, Array(priCol))(config)
  def apply(name:String, cols:Cols)(implicit config:TraitDBConfig):DBManager = apply(name, cols, noCols)(config)
  var loadedDBManagers = Map[String, DBManager]()
  private def addLoadedDBManager(dbm:DBManager, dbmID:String, trace:String) = {
    loadedDBManagers += (dbmID -> dbm)
    loadedDBManagersTrace += (dbmID -> trace)
  }
  def apply(table:Table)(implicit config:TraitDBConfig) = new DBManager(table)(config)
  var loadedDBManagersTrace = Map[String, String]() // stackTrace
}
/* Database functionality for making the following SQL queries: SELECT, DELETE, INSERT and UPDATE */
class DBManager(val table:Table)(implicit val dbConfig:TraitDBConfig = DefaultDBConfigFromFile) extends DBManagerDDL(table:Table, dbConfig:TraitDBConfig) with JsonFormatted {
  implicit val dbDML:DBManagerDML = this
  import table._
  val tableID = table.tableName+"_"+shaSmall(table.tableName+"|"+dbConfig.dbname+"|"+dbConfig.dbhost+"|"+dbConfig.dbms)
  val keys = Array("tableName", "dbname", "host", "dbms", "tableID", "numCols")
  val vals = Array[Any](table.tableName, dbConfig.dbname, dbConfig.dbhost, dbConfig.dbms, tableID, tableCols.size)
  
  if (dbConfig.dbms == "postgresql") {  // new version supports??
    using(connection){  // postgresql does not support blob. So we create an alias from blob to bytea
      conn => using(conn.prepareStatement(Table.createPostGreBlob))(_.executeUpdate)
    }
  }
  def getTableID = DBManager.loadedDBManagers.find(_._2 eq this) match {
    case Some((id, _)) => id
    case _ => throw new DBException("DBManger not registered: "+this)
  }
  // private fields
  private lazy val db = new DB(dbConfig)
  def connection = db.getConnection
  ////////////////////////////////////////////////////////////////
  //// DML/DDL METHODS START
  ////////////////////////////////////////////////////////////////
   /**
     * Inserts an entry (row) into the table
     *    @param data is an array giving the values to insert in the Cols (columns). Every column must be filled.
     *    @return the number of rows affected by the query
     */ /* to override in SecureDBManager */
  // note rs below must be plaintext
  def insertRS[T](rs:ResultSet) = bmap(rs.next)(insertArray(getTable.tableCols.map(get(rs, _)))).size   
  def exists(wheres:Wheres) = countRows(wheres) >= 1
  def exists(wheres:Where*):Boolean = exists(wheres.toArray)
  def isNonEmpty = exists()
  def isEmpty = !isNonEmpty

  def aggregateBigInt(aggregates:Aggregates, wheres:Wheres, havings:Havings = Array()) = aggregate(aggregates, wheres, havings, (rs, s) => BigInt(rs.getString(s)))(BigInt(_))
  def aggregateBigDecimal(aggregates:Aggregates, wheres:Wheres, havings:Havings = Array()) = aggregate(aggregates, wheres, havings, (rs, s) => BigDecimal(rs.getBigDecimal(s)))(BigDecimal(_))
  def aggregateLong(aggregates:Aggregates, wheres:Wheres, havings:Havings = Array()) = aggregate(aggregates, wheres, havings, (rs, s) => rs.getLong(s))(_.toLong)
  def aggregateDouble(select:Aggregates, wheres:Wheres, havings:Havings = Array()) = aggregate(select, wheres, havings, (rs, s) => rs.getDouble(s))(_.toDouble)
  def aggregateInt(select:Aggregates, wheres:Wheres, havings:Havings = Array()) = aggregate(select, wheres, havings, (rs, s) => rs.getInt(s))(_.toInt)

  // is deleteAll really necessary?
  def deleteAll:Int = delete(Array()) // deletes all rows from the table. returns the number of rows deleted (Int)

  def aggregateGroupBigInt(aggregates:Aggregates, wheres:Wheres, groups:GroupByIntervals=Array(), havings:Havings=Array()) = 
    aggregateGroupHaving(aggregates, wheres, groups, havings, (rs, s) => BigInt(rs.getString(s)))(BigInt(_))
  def aggregateGroupBigDecimal(aggregates:Aggregates, wheres:Wheres, groups:GroupByIntervals=Array(), havings:Havings=Array()) = 
    aggregateGroupHaving(aggregates, wheres, groups, havings, (rs, s) => BigDecimal(rs.getBigDecimal(s)))(BigDecimal(_))
  def aggregateGroupLong(aggregates:Aggregates, wheres:Wheres, groups:GroupByIntervals=Array(), havings:Havings=Array()) = 
    aggregateGroupHaving(aggregates, wheres, groups, havings, (rs, s) => rs.getLong(s))(_.toLong)
  def aggregateGroupDouble(aggregates:Aggregates, wheres:Wheres, groups:GroupByIntervals=Array(), havings:Havings=Array()) = 
    aggregateGroupHaving(aggregates, wheres, groups, havings, (rs, s) => rs.getDouble(s))(_.toDouble)
  def aggregateGroupInt(aggregates:Aggregates, wheres:Wheres, groups:GroupByIntervals=Array(), havings:Havings=Array()) = 
    aggregateGroupHaving(aggregates, wheres, groups, havings, (rs, s) => rs.getInt(s))(_.toInt)
  
  // following for single where
  def countRows(where:Where):Long = countRows(Array(where))
  
  def countAllRows = countRows(Array[Where]())
  
  // following for single where, single Increment
  @deprecated def incrementCol(where:Where, increment:Increment) = incrementCols(Array(where), Array(increment))
  def incrementColTx(where:Where, increment:Increment) = incrementColsTx(Array(where), Array(increment))

  // following for single where, single update
  def updateCol(where:Where, update:Update[Any]):Int = updateCols(Array(where), Array(update))

  def insertList(data:List[Any]):Int = insertArray(data.toArray)  
   
  // hack below. (why?)
  // an implementation of func2 needs to be provided to convert the result to the intended type (i.e. Long, Int, Double BigInt, etc
  // this is done in aggregateLong, aggregateInt, etc
  private def aggregate[T](select:Aggregates, wheres:Wheres, havings:Havings,func: (RS, String)=>T)(implicit func2:String => T = ???) = {  // to provide implementation for converting to Scala Number types
    val data = aggregateGroupHaving(select, wheres, Array(), havings, func)(func2)
    if (data.size != 1) throw DBException("Aggregate query returned "+data.size+" rows. Require exactly one row for this operation")
    data(0)
  }
  
  //////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////
  // DDL related stuff below
  //////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////

  // initialize table
  createTableIfNotExists // create table if it does not exist
  validateTableSchema // see if schema of actual table matches schems here
  // methods start
  def dropTable:Int = using(connection) { conn => using(conn.prepareStatement(dropSQLString)){st => st.executeUpdate} }
  def getTable = table
  def getConfig = dbConfig
  private def createTableIfNotExists = using(connection){
    conn => using(conn.prepareStatement(createSQLString))(_.executeUpdate)
  }
  // GENERAL TABLE VALIDATION
  private lazy val schemaStr = if (dbConfig.dbms == "postgresql") postgreSQLSchemaStr else mySQLSchemaStr  
  private def validateTableSchema = using(connection){conn =>
    using(conn.prepareStatement(schemaStr)){st => {
        using (st.executeQuery) {rs => 
          val rsmd = rs.getMetaData();
          val numberOfColumns = rsmd.getColumnCount();
          val colNames = for (i <- 1 to numberOfColumns) yield rsmd.getColumnName(i).toUpperCase
          var colVals:List[Seq[String]]=Nil
          while (rs.next()) colVals ::= (for (i <- 1 to numberOfColumns) yield rs.getString(i).toUpperCase) 
          validateSchema(colNames.indices.map(i => colNames(i) -> colVals.map(_(i))).toMap)
        }
      }
    }
  }  
  private def checkH2 = if (dbConfig.dbms != "h2") throw new DBException("Only H2 currently supported for this command")
  def updatePassword(newPass:String) = {
    checkH2
    using(connection){conn =>
      using(conn.prepareStatement(Table.getH2PassString(dbConfig.dbuser, newPass))){st => {
         st.executeUpdate
        }
      }
    }
    "Ok"
  }
  // inefficient way to copy
  /**
   * this is inefficient. It inserts one row at a time
   */
  @deprecated def copyTo(dest:DBManager) = copyFromTo(this, dest)
  //////////////////////////////////////////////////////////////////
  // does not support secure methods .. export/import will be encrypted in securedb
  def exportAllToCSV(file:String) = exportToCSV(file, Array())
  def exportToCSV(file:String, wheres:Wheres) = {
    checkH2
    if (new File(file).exists) throw new DBException("dest file should not exist")
    val debugStr = getH2ExportStr(file, wheres)
    using(connection){conn =>
      using(conn.prepareStatement(getH2ExportStr(file, wheres))) { st => 
        setData(getWheresData(wheres), 0, st, debugStr)
        //setWheres(wheres, 0, st, debugStr)
        st.executeUpdate
      } 
    }
  }
  def exportToCSV(file:String, cols:Cols, wheres:Wheres) = {
    checkH2
    if (new File(file).exists) throw new DBException("dest file should not exist")
    using(connection){conn =>
      using(conn.prepareStatement(getH2ExportStr(file, cols, wheres))) { st => 
        val (setCtr, debugStr) = setData(cols.flatMap(_.compositeColData) ++ getWheresData(wheres), 0, st, getH2ExportStr(file, wheres))
        st.executeUpdate
      } 
    }
  }
  def importFromCSV(file:String) = {
    checkH2
    using(connection){conn => 
      using(conn.prepareStatement(table.getH2ImportStr(file))) { st => 
        st.executeUpdate
      } 
    }
  }
  //////////////////////////////////////////////////////////////////
  def exportAllEncrypted(file:String, secretKey:String) = exportEncrypted(file, secretKey)
  def exportEncrypted(file:String, secretKey:String, wheres: Where *):Int = exportToEncryptedCSV(this, file, secretKey, wheres.toArray)
  def importEncrypted(file:String, secretKey:String) = importFromEncryptedCSV(this, file, secretKey)
  DBManager.addLoadedDBManager(this, tableID, Thread.currentThread.getStackTrace.toList.take(4).map(_.toString).reduceLeft(_+","+_))
  
  
  
}



   
   
   
   
   
   
   
   
   
   
   
   
   
   



