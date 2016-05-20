package mux.db

import amit.common.file.TraitFilePropertyReader
import amit.common.json.JSONUtil
import amit.common.Util._
import java.io.File
import mux.db.config.DBConfigFromFile
import mux.db.core.DataStructures._
import mux.db.core.Util._


case class GroupBy(dbHost:String, dbms:String, dbname:String) extends JSONUtil.JsonFormatted {
  val keys = Array("dbHost":String, "dbms":String, "dbname":String)
  val vals = Array[Any](dbHost:String, dbms:String, dbname:String)
}
case class DBMgrData(id:String, dbMgr:DBManager) extends JSONUtil.JsonFormatted {
  val tableName = dbMgr.table.tableName
  val keys = Array("id", "tableName")
  val vals = Array[Any](id, tableName)
  val conf = dbMgr.dbConfig
  val getGroupBy = GroupBy(conf.dbhost, conf.dbms, conf.dbname)
}
case class DBGroup(index:Int, groupBy:GroupBy, dbMgrs:Array[DBManager]) extends JSONUtil.JsonFormatted {
  val validateUserIDsPasswords = if (dbMgrs.size == 0) true else {
    val c0 = dbMgrs(0).dbConfig
    dbMgrs.map(_.dbConfig).forall(c => c.dbname == c0.dbname && c.dbpass == c0.dbpass)
  }
  val getGroupSummary = new JSONUtil.JsonFormatted {
    val keys = groupBy.keys ++ Array("groupID", "validation_passed", "configSrc")
    val vals = groupBy.vals ++ Array(index, validateUserIDsPasswords, dbMgrs.map(_.dbConfig.configSource).toSet.toArray)
  }
  val tableNames = dbMgrs.map(_.table.tableName)
  val keys = getGroupSummary.keys ++ Array("tables") 
  val vals = getGroupSummary.vals ++ Array[Any](JSONUtil.encodeJSONArray(tableNames)) 
  def updatePassword(oldPassword:String, newPassword:String, checkOldPassword:Boolean) = {    
    if (dbMgrs.size == 0) throw new Exception("Empty dbName group with groupID: "+index)
    if (!validateUserIDsPasswords) throw new Exception("Usrname/password different in group with groupID: "+index)
    if (checkOldPassword && dbMgrs(0).dbConfig.dbpass != oldPassword) throw new Exception("oldPassword does not match: "+index)
    dbMgrs(0).updatePassword(newPassword)
    val configsToUpdate = dbMgrs.map(_.dbConfig).filter(_.configSource.startsWith("file")).toSet
    configsToUpdate.foreach{c =>
      val cProps = c.asInstanceOf[DBConfigFromFile]
      cProps.write("dbpass", newPassword, "changed_by_DBMaintenance_tool", true)
      if (amit.common.Util.debug) println("writing to "+c.configSource)
    }
    configsToUpdate.map(_.configSource).toArray
  }
}
class DBMaintenanceUtil(val objects:Seq[AnyRef]) extends TraitFilePropertyReader {
  val propertyFile = "dbMaintenance.properties"
  val masterPassword = read("masterPassword", "some_password")
  val dbGroups = objects
  var autoBackupEnabled = read("autoBackupEnabled", false)
  var autoBackupMillis = read("autoBackupMillis", 1800000L) // 1800000 = 30 mins
  var autoBackupDirectory = read("autoBackupDirectory", "h2_backup_DB") 
  var lastBackup = 0L
  
  def getDBMgrs = {
    if (dbGroups.size < 1) throw new Exception("no db groups defined") // dummy access
    // dummy access above, to ensure all dbs are loaded      
    DBManager.loadedDBManagers.toArray.map{
      case (id, db)=> DBMgrData(id, db) 
    }
  }  
}
class DBMaintenance(dbu:DBMaintenanceUtil) { // objects will be accessed to load the DBManagers in them. They just need to be accessed as in the next line
  def this(objects:Seq[AnyRef]) = this(new DBMaintenanceUtil(objects)) // objects will be accessed to load the DBManagers in them. They just need to be accessed as in the next line
  import dbu._
  import DBManager._
  // allow for changing password, migrating db etc
  def getTableIDs = getDBMgrs//.getDBMgrs
  def getTableIDsSorted = getDBMgrs.sortWith{(l, r) =>
    l.dbMgr.getTable.tableName < r.dbMgr.getTable.tableName
  }
  
  def autoBackupGet = autoBackupEnabled
  def autoBackupSet(enabled:Boolean) = {    
    autoBackupEnabled = enabled
    autoBackupGet
  }
  def autoBackupDirectorySet(dir:String) = { 
    autoBackupDirectory = dir
    // add code to validate backup
    autoBackupDirectoryGet 
  }
  def autoBackupDirectoryGet = autoBackupDirectory
  def autoBackupMillisGet = autoBackupMillis
  def autoBackupMillisSet(millis:Long) = {
    if (millis < 300000) throw DBException(s"auto backup millis must be > 300000 (5 mins)") // 1 min
    autoBackupMillis = millis
    autoBackupMillisGet
  }
  
  def backupLoadedDBMgrs(dir:String) = {
    val dirFile = new File(dir)
    if (dirFile.isFile) throw new DBException("[dir] is a file")
    dirFile.mkdir
    if (!dirFile.isDirectory) throw new DBException("[dir] not found")
    
    val time = getTime
    getDBNameGroupsDetails.map{
      case DBGroup(index:Int, GroupBy("localhost", "h2", db), dbMgrs:Array[DBManager]) => 
        val start = getTime
        val humanTime = toDateString(time).replace(':', '_').replace(' ', '_')
        val fileName = s"${dir}/${db}_${time}_(${humanTime}).zip" // dir+"/"+db+"_"+time+".zip"        
        val sqlString = "BACKUP TO ?"
        if (printSQL_?) println("BACKUP query SQL [?]:\n  "+sqlString)
        
        using(dbMgrs(0).connection.prepareStatement(sqlString)){st =>           
          val (_, str) = dbMgrs(0).setData(Array((fileName, VARCHAR(255))), 0, st, sqlString)            
          if (printSQL) println("BACKUP query SQL [R]:\n  "+str)
          st.executeUpdate
        }
        
        val backTimeMillis = getTime-start
        new JSONUtil.JsonFormatted{
          val keys = Array("db", "backupTimeMillis", "numTables", "tables", "fileName", "dir", "startTime", "humanStartTime")
          val vals = Array(db, backTimeMillis, dbMgrs.size, dbMgrs.map(_.getTable.tableName), fileName, dir, time, toDateString(time))
        }
      case any => throw new DBException("unsupported backup config: "+any)
    }
  }

  def getDuplicateTableNames = getTableIDs.groupBy(_.tableName.toLowerCase).filter{_._2.size > 1}.values.toArray.map(_.toList)
  def getInitStackTrace(tableID:String) = loadedDBManagersTrace.get(tableID).getOrElse(throw DBException("no such tableID trace: "+tableID)).split(",")
  def getCreateStrings = getDBMgrs.map(_.dbMgr.table.createSQLString)
  def getCreateString(tableID:String) = getTable(tableID).createSQLString
  def getDBNameGroups = getDBNameGroupsDetails.map(_.getGroupSummary)
  def getDBNameGroupsDetails = {
    val dbMgrs = getDBMgrs
    dbMgrs.groupBy(_.getGroupBy).toArray.zipWithIndex.map{
      case ((groupBy, dbs), i) => DBGroup(i, groupBy, dbs.map(_.dbMgr))
    }
  }
  def changePassword(groupID:Int, oldPassword:String, newPassword:String) = {    
    val groups = getDBNameGroupsDetails
    if (groupID < groups.size && groupID >= 0) {    
      groups(groupID).updatePassword(oldPassword, newPassword, true)
    } else throw new Exception("Invalid groupID: "+groupID)
  }
  def resetPassword(groupID:Int, masterPassword:String, newPassword:String) = {    
    val groups = getDBNameGroupsDetails
    if (masterPassword != dbu.masterPassword) throw new Exception("Incorrect master password")    
    if (groupID < groups.size && groupID >= 0) {    
      groups(groupID).updatePassword("", newPassword, false)
    } else throw new Exception("Invalid groupID: "+groupID)
  }
  // also validate that userNames and passwords across same group are same
  def getDB(tableID:String) = loadedDBManagers.get(tableID) match {
    case Some(dbm) => dbm
    case _ => throw DBException("table not found for ID: "+tableID)      
  }  
  def getConfig(tableID:String) = {
    val db = getDB(tableID)
    import db.dbConfig._
    new JSONUtil.JsonFormatted {
      val keys = Array  ("host", "dbname", "dbms", "userID")
      val vals = Array[Any](dbhost, dbname, dbms, dbuser)
    }
  }

  def getTable(tableID:String) = getDB(tableID).table
  def countAllRows(tableID:String) = getDB(tableID).countAllRows
  import BetterDB._
  def getAllRows(tableID:String) = {
    val db = getDB(tableID)
    val cols = db.table.tableCols
    val ordering = cols.find(_.name.toLowerCase == "time") match {
      case Some(col) => Array(Ordering(col, Decreasing))
      case _ => Array[Ordering]()
    }    
    getRows(
      cols, 
      db.select(cols).orderBy(ordering).asList
    )
  }  
  // def getAllRowsAlt(tableID:String) = getDB(tableID).selectAll4
  
  def getWhere(tableID:String, colName:String, operation:String, data:String) = {
    
    val $info$ = "This tests whether a where clause is valid or not. Does not return any data."
    val table = getTable(tableID)    
    table.tableCols.find(_.name.toLowerCase == colName.toLowerCase) match {
      case Some(col) => Where(col, getOp(operation), castData(col.colType, data))
      case None => throw DBException("column not found: "+colName)
    }
  }
  def getAggregate(tableID:String, colName:String, aggregate:String) = {
    val table = getTable(tableID)    

    table.tableCols.find(_.name.toLowerCase == colName.toLowerCase) match {
      case Some(col) => Aggregate(col, getAggr(aggregate))
      case None => throw DBException("column not found: "+colName)
    }
  }
  def getAvailableOps = allOps
  def getAvailableAggregates = allAggrs
  def getAggregatedColFiltered(tableID:String, aggregateCol:String, aggregate:String, whereColName:String, whereOp:String, whereData:String) = {
    val wheres = Array(getWhere(tableID:String, whereColName:String, whereOp:String, whereData:String))
    val aggr = Array(getAggregate(tableID, aggregateCol, aggregate))
    val db = getDB(tableID)
    db.aggregateLong(aggr, wheres)(0)
  }    
  def getAggregatedCol(tableID:String, aggregateCol:String, aggregate:String) = {
    val aggr = Array(getAggregate(tableID, aggregateCol, aggregate))
    val db = getDB(tableID)
    db.aggregateLong(aggr, Array())(0)
  }    
  def getRowsWhere(tableID:String, colName:String, operation:String, data:String) = {
    val wheres = Array(getWhere(tableID:String, colName:String, operation:String, data:String))
    val db = getDB(tableID)
    getRows(db.table.tableCols, db.selectStar.where(wheres: _*).asList)      
  }    
  def countRowsWhere(tableID:String, colName:String, operation:String, data:String) = getDB(tableID).countRows(getWhere(tableID:String, colName:String, operation:String, data:String))
  
  def deleteRowsWhere(tableID:String, colName:String, operation:String, data:String, masterPassword:String) = {
    if (masterPassword != dbu.masterPassword) throw new Exception("Incorrect master password")    
    val wheres = Array(getWhere(tableID:String, colName:String, operation:String, data:String))
    getDB(tableID).delete(wheres)
  }      
  doRegularly(
    {
      if (autoBackupEnabled) {
        val start = getTime
        if ((start - lastBackup) > autoBackupMillis) {
          if (debug) println (" [INFO DB_AUTO_BACKUP STARTED]")
          tryIt(backupLoadedDBMgrs(autoBackupDirectory))
          val end = getTime
          if (debug) println (s" [INFO DB_AUTO_BACKUP ENDED] Time elapsed: ${(end - start)/1000d} s ")
          lastBackup = end
        }
      }
    },
    OneMin
  )
                               
}














