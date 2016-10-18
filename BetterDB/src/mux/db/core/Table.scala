package mux.db.core

import DataStructures._
import Util._
import mux.db.core.FKDataStructures._
import amit.common.Util._

case class Table(tableName:String, tableCols:Cols, priKey:Cols) {
  import Table._
  if (Col.reservedNames.contains(tableName.toUpperCase)) throw DBException(s"table name $tableName is a reserved word")

  val colNames = tableCols.map(_.name.toUpperCase) 
  val priKeyColNames = priKey.map(_.name.toUpperCase) 
  if (colNames.distinct.size != colNames.size) throw new DBException("colNames should not contain duplicates. Rename some cols and retry.")
  if (priKeyColNames.distinct.size != priKeyColNames.size) throw new DBException("priKeyColNames should not contain duplicates. Rename some cols and retry.")

  if (!priKeyColNames.forall(colNames.contains)) throw new DBException("priKeyColNames should be present in colNames.")
  def this(tableName:String, col:Col, priKey:Col) = this(tableName, Array(col), Array(priKey))

  override def toString = tableName

  def assertColExists(c:Col) = c.compositeCols.foreach{col =>
    assert(containsCol(col), "Table ["+col.optTable.getOrElse(tableName)+"] does not have column: "+col.name)
  }
  private def containsCol(anyCol:Col) = {
    anyCol.colType match {
      case CONST(_, _) => true
      case _ => anyCol.optTable.getOrElse(this).tableCols.contains(anyCol.simpleCol)
    }    
  }
  
  def createSQLString = "CREATE TABLE IF NOT EXISTS "+tableName+
    " ("+tableCols.map(f => f.name+" "+colTypeString(f.colType)).reduceLeft(_+", "+_)+priKeyString+checkString+")"
  private def checkString = tableCols.filter(
    _.colType match {
      case ULONG(_) | UINT(_) => true
      case _ => false
    }
  ) match {
    case f if f.size > 0 => ","+f.map("CHECK("+_.name+" >= 0)").reduceLeft(_+","+_) 
    case _ => ""
  }
  private def priKeyString = if (priKey == null || priKey.size == 0) "" else ", PRIMARY KEY ("+priKey.map(_.name).reduceLeft(_+", "+_)+")"

  def insertSQLString = "INSERT INTO "+tableName+" VALUES("+ tableCols.map(_ => "?").reduceLeft(_+", "+_)+ ")"
  def dropSQLString = "DROP TABLE "+tableName
  def deleteSQLString(search:Wheres) = "DELETE FROM "+tableName+whereString(search, "WHERE", "and")

  // selecting rows; 
  def selectSQLString(selectCols:Cols, wheres:Wheres)(implicit orderings:Orderings=Array(), limit:Int = 0, offset:Long = 0) = {
    "SELECT "+getSelectString(selectCols)+
    " FROM "+getTableNames(selectCols, wheres)+whereString(wheres, "WHERE", "and")+getOrderLimitSQLString(orderings,limit, offset)
  }
  def insertIntoSQLString(table:Table, selectCols:Cols, wheres:Wheres)(implicit orderings:Orderings=Array(), limit:Int = 0, offset:Long = 0) = {
    "INSERT INTO "+table.tableName+" "+selectSQLString(selectCols:Cols, wheres:Wheres)
  }

  def getTableNames(selectCols:Cols, wheres:Wheres) = {
    val selectTables = selectCols.flatMap(_.compositeTableNames).toSet
    
    val whereTables = wheres.flatMap(_.col.compositeTableNames).toSet 
    val dataTables = wheres.collect{
      case Where(_, _, col:Col) => col.compositeTableNames
    }.flatten.toSet

    val allTables =   wheres.flatMap(_.nestedWhereTables) ++ (selectTables ++ whereTables ++ dataTables + tableName) // add this table name too (last item)
    allTables.reduceLeft(_+","+_)
  }
  // counting rows
  def countSQLString(wheres:Wheres) = "SELECT COUNT(*) FROM "+ getTableNames(Array(), wheres) + whereString(wheres, "WHERE", "and") 
  
  /**
   * in the following the toOp gives the method of update for the cols (to be applied on update:Cols below). 
   * For example in normal update 
   *  UPDATE Foo SET bar = "123" WHERE baz = "43"  
   * the other way this can be used is for increment (See incrementString below)   
   *  UPDATE Foo SET bar = bar + 123" WHERE baz = "34"
   * 
   * this op will be different in different cases
   */
  def updateSQLString[T](wheres:Wheres, updates:Updates[T])(implicit toWhere:Update[_] => Where = upd => Where(upd.col, Eq, upd.data)) = {  ///////// TO CHECK IF NOTIMPL IS EVER CALLED
    assert(updates.size > 0)
    assert(wheres.size > 0)
    "UPDATE "+tableName+whereString(updates.map(toWhere), "SET", ",")+whereString(wheres, "WHERE", "and")
  }
  
  ////////////////////////////////////////
  // aggregates 
  ////////////////////////////////////////
  private def aggregateString(aggregates: Aggregates) = {
    // skipping Top as its a syntactic sugar added by our library and unsupported natively
    assert(aggregates.size > 0, "aggregate ops must be on one or more cols")
    aggregates.filter{
      case Aggregate(_, Top(_)|GroupBy) => false // GroupBy should not generate SQL query
      case _ => true 
    }.map{x => 
      x.aggrSQLString+" as "+x.alias
    }.reduceLeft(_+","+_)
    // use following when using h2 (SUBSTRING_INDEX)
    //    aggregates.map(x => getAggrOpStr(x.aggr, x.col.colName)+" as "+x.col.colName+x.aggr).reduceLeft(_+","+_)
  }  
  // following only works for MySql, (SUBSTRING_INDEX is not supported in h2)
  //  private def getAggrOpStr(aggr:Aggr, name:String) = {
  //    val str = aggr+"("+name+")"
  //    aggr match {
  //      case First => "SUBSTRING_INDEX("+str+"',',1)"
  //      case Last => "SUBSTRING_INDEX("+str+"',',-1)"
  //      case _ => str 
  //    }
  //  }
  
  
  ////////////////////////////////////////
  // aggregates with intervals (find avg(age) in each age_group (0-10, 10-20, 20-30, 30-40)... 
  // intervals need to be equal!
  // intervals can only be applied to Numeric type (INT, LONG, ULONG, UINT). (U)ScalaBIGINT is not supported.
  //  uses the idea from http://stackoverflow.com/a/197300/243233
  ////////////////////////////////////////
  //
  def aggregateSQLString(aggregates:Aggregates, wheres:Wheres, groupByIntervals:GroupByIntervals, havings:Havings) = {
    //"SELECT "+getOpStr(select)+intervalString(groupByIntervals)+" FROM "+tableName+ 
    val tableNames = getTableNames(aggregates.map(_.col), wheres)
    //SELECT MAX(USERS.SAL) as LjkkudMSTzMAX,((USERS.AGE + USERS.SAL) + 4) as uOzFPSGTyfinterval0 FROM USERS WHERE USERS.SAL > 10000 GROUP BY uOzFPSGTyfinterval0
    "SELECT "+
    aggregateString(aggregates)+ // MAX(USERS.SAL) as LjkkudMSTzMAX
    intervalString(groupByIntervals)+ // ((USERS.AGE + USERS.SAL) + 4) as uOzFPSGTyfinterval0
    " FROM "+
    tableNames+ // USERS
    whereString(wheres, "WHERE", "and")+ // USERS.SAL > 10000
    groupByString(groupByIntervals)+ // GROUP BY uOzFPSGTyfinterval0
    havingString(havings)
  }
  private def intervalString(groupByIntervals:GroupByIntervals) =  
    if (groupByIntervals.size == 0) "" else 
    ","+groupByIntervals.map{g => 
      (
        if (g.interval != 0) 
          g.interval+"*round("+g.col.colSQLString+"/"+g.interval+".0, 0)"
        else g.col.colSQLString // ((USERS.AGE + USERS.SAL) + 4)
      )+" as "+g.alias //  as uOzFPSGTyfinterval0
    }.reduceLeft(_+","+_)
  private def groupByString(groupByIntervals:GroupByIntervals) = 
    if (groupByIntervals.size == 0) "" else " GROUP BY "+groupByIntervals.map(g => g.alias).reduceLeft(_+","+_)
  
  private def havingString(havings:Havings) = 
    if (havings.size == 0) "" else " HAVING "+havings.map(h => h.havingSQLString).reduceLeft(_+" AND "+_)
  
  ////////////////////////////////////////
  // following is used for incrementing a single col
  ////////////////////////////////////////
  def incrementColsString(wheres:Wheres, increments:Increments) = 
    updateSQLString(wheres, increments)(increment => Where(increment.col, IncrOp(increment), increment.data)) 

  lazy val postgreSQLSchemaStr = getPostgreSQLSchemaStr(tableName)
  lazy val mySQLSchemaStr = getMySQLSchemaStr(tableName)
  def validateSchema(colMap:Map[String, List[String]]) = validateTableSchema(colMap, tableCols, priKey, tableName)
  /**
   * http://stackoverflow.com/a/2499396/243233
   * http://stackoverflow.com/a/16792904/243233
   * http://stackoverflow.com/a/14072931/243233
   */
  
  ////////////////////// Foreign Key constraints //////////////////////////
  def getFKConstraintStr(link:Link) = "SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS WHERE "+
    "CONSTRAINT_NAME = '"+getFKConstraintName(link)+"'"
  def getFKConstraintStrPostgreSQL(link:Link) = "SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE "+
    "CONSTRAINT_NAME = '"+getFKConstraintName(link)+"'"
  def getFKConstraintName(link:Link) = ("fk_"+shaSmall(tableName+"_"+
                                                       link.pkTable+"_"+
                                                       link.pkCols.map(_.colSQLString).reduceLeft(_+"_"+_)+"_"+
                                                       link.fkCols.map(_.colSQLString).reduceLeft(_+"_"+_))).toUpperCase
  def dropFkLinkString(link:Link) = 
    "ALTER TABLE "+tableName+
    " DROP CONSTRAINT "+getFKConstraintName(link)
  
  def createFkLinkString(link:Link) = 
    "ALTER TABLE "+tableName+
    " ADD CONSTRAINT "+getFKConstraintName(link)+
    " FOREIGN KEY ("+link.fkCols.map(_.colSQLString).reduceLeft(_+","+_)+") REFERENCES "+
    link.pkTable.tableName+"("+link.pkCols.map(_.colSQLString).reduceLeft(_+","+_)+") ON DELETE "+link.rule.onDelete+" ON UPDATE "+link.rule.onUpdate  
  ///////////////////////////////////////////////////////////////////
  
  
  
  ////////////////////// INDEX constraints //////////////////////////

  def createIndexString(cols:Cols) = {
    "CREATE INDEX IF NOT EXISTS "+getIndexName(cols:Cols)+" ON "+tableName+" ("+cols.map(_.colSQLString).reduceLeft(_+","+_)+")"
  }
  def dropIndexString(cols:Cols) = {
    "DROP INDEX IF EXISTS "+getIndexName(cols:Cols)
  }
  
  def getIndexName(cols:Cols) = {
    if (cols.isEmpty) throw DBException("at least one column is needed for index")
    val name = shaSmall(tableName+"_"+cols.map(_.alias).reduceLeft(_+_))
    name
  }
  //////////////////////////////////////////////////////////////////
  
  def getH2ExportStr(file:String, cols:Cols, wheres:Wheres) = "CALL CSVWRITE('"+file+"', '"+selectSQLString(cols, wheres)+"')"
  def getH2ExportStr(file:String, wheres:Wheres):String = getH2ExportStr(file, Array(), wheres)
  def getH2ImportStr(file:String) = "INSERT INTO "+tableName+" SELECT * FROM CSVREAD('"+file+"')"
    
}
object Table {
  def apply(tableName:String, col:Col, priKey:Col) = new Table(tableName, Array(col), Array(priKey))
  def apply(tableName:String, tableCols:Cols) = new Table(tableName, tableCols, Array[Col]())
  def apply(tableName:String, tableCols:Col*):Table = apply(tableName, tableCols.toArray)
  // def apply(tableName:String, tableCols:Cols, priKeyCols:Col*):Table = new Table(tableName, tableCols.toArray, priKeyCols.toArray)    

  def createPostGreBlob = "CREATE DOMAIN BLOB as BYTEA"
  private def getOrdering(isDescending:Boolean) = if (isDescending) " DESC" else ""
  private def getOrderStr(orderings:Orderings) = {
    assert(orderings.size > 0)      // "ORDER BY supplier_city DESC, supplier_state ASC;"
    " ORDER BY "+orderings.map{
      x => x.col.colSQLString+getOrdering(x.isDescending)
    }.reduceLeft(_+","+_)
  }
  private def getOrderLimitSQLString(orderings:Orderings, limit:Int, offset:Long) = {
    (if (orderings.size == 0) "" else getOrderStr(orderings)) + 
    (if (limit == 0) "" else " LIMIT "+limit) +
    (if (offset == 0) "" else " OFFSET "+offset)
  }
  private def whereString(wheres:Wheres, str:String, joinOp:String) = if (wheres.size == 0) "" else {
     " "+str+" "+wheres.map{_.whereSQLString}.reduceLeft(_+" "+joinOp+" "+_)
  }

  private def getSelectString(cols:Cols) = if (cols.size == 0) "*" else cols.map(col => col.colSQLString +" AS "+col.alias).reduceLeft(_+","+_)
  private def getPostgreSQLSchemaStr(tableName:String) = 
    """SELECT c.COLUMN_NAME as COLUMN_NAME, c.DATA_TYPE as TYPE, 
          CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'PRI' ELSE '' END AS KEY
          FROM INFORMATION_SCHEMA.COLUMNS c 
          LEFT JOIN (
                      SELECT ku.TABLE_CATALOG,ku.TABLE_SCHEMA,ku.TABLE_NAME,ku.COLUMN_NAME
                      FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                      INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
                          ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                          AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                   )   pk 
          ON  c.TABLE_CATALOG = pk.TABLE_CATALOG
                      AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA
                      AND c.TABLE_NAME = pk.TABLE_NAME
                      AND c.COLUMN_NAME = pk.COLUMN_NAME
          WHERE c.TABLE_NAME='"""+tableName.toLowerCase+"'";

  private def getMySQLSchemaStr(tableName:String) = "SHOW COLUMNS FROM "+tableName.toLowerCase
  private def validateTableSchema(colMap:Map[String, List[String]], cols:Cols, priKeys:Cols, tableName:String) = {
    def sameType(t:String, col:Col) = (t, colTypeString(col.colType)) match {
      case (t1, t2) if (t1 == t2) => true
      case (t1, t2) if (t1.startsWith("BIGINT") && t2.startsWith("BIGINT")) => true
      case (t1, t2) if (t1.startsWith("CHARACTER VARYING") && t2.startsWith("VARCHAR")) => true // for postgreSQL
      case (t1, t2) if (t1 == "VARCHAR(2147483647)" && t2 == "VARCHAR") => true // for h2 (possibly MySQL)
      case (t1, t2) if (t1.startsWith("INTEGER") && t2.startsWith("INT")) => true // for postgreSQL
      case (t1, t2) if (t1.startsWith("BLOB") && t2.startsWith("BLOB")) => true 
      case (t1, t2) if (t1.startsWith("BYTEA") && t2.startsWith("BLOB")) => true 
      case (t1, t2) => println("t1["+t1+"], t2["+t2+"]"); false    
    }
    val indices = cols.indices
    val names = colMap.get("COLUMN_NAME").get
    val types = colMap.get("TYPE").get
    val keys = colMap.get("KEY").get zip names filter (_._1 == "PRI") map (_._2)
    val keyMatch = keys.size == priKeys.size && priKeys.forall(x => keys.contains(x.name.toUpperCase))
    if (!keyMatch) throw DBException("table schema mismatch: (primary key) "+tableName+" found ["+keys+"], Required ["+priKeys.toList+"]")
    val tmpMap = (names zip types) map (x => (x._1, x._2, cols.find(_.name.toUpperCase == x._1)))
    val nameMatch = tmpMap.forall(_._3.isDefined)
    if (!nameMatch) throw DBException("table schema mismatch: (name) "+tableName+" found ["+names+"]. Required ["+cols.toList+"]")
    val typeMatch = nameMatch && tmpMap.forall(x => sameType(x._2, x._3.get))
    if (!typeMatch) throw DBException("table schema mismatch: (type) "+tableName+" found ["+types+"]. Required ["+cols.toList.map(x => colTypeString(x.colType))+"]") 
    val numberMatch =  tmpMap.size == cols.size
    if (!numberMatch) throw DBException("table schema mismatch: (wrong No. of columns) "+tableName+" found["+tmpMap.size+"], ["+names+"]. Required["+cols.size+"]:["+cols.toList.map(_.name)+"]")
  }
  def getH2PassString(user:String, pass:String) = "ALTER USER "+user+" SET PASSWORD '"+pass+"'"
  
}
