package mux.db

import mux.db.config.TraitDBConfig
import mux.db.core.DataStructures._
import mux.db.core._
import mux.db.core.Util._
import amit.common.Util._
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.{ResultSet => RS}

abstract class DBManagerDML(table:Table, dbConfig:TraitDBConfig) {
  protected def connection:Connection
  import table._
  // maps cols without table names (i.e., from this table) to cols with table names
  // e.g. if this table is T1 then cols in selectCols without optTable.isDefined will have optTable changed to T1.
  private def canonical(c:Col):Col = c.to(table)
  private def canonical(cols:Cols):Cols = cols.map(canonical)
  private def canonical(w:Where):Where = w.to(table)  
  private def canonical(wheres:Wheres):Wheres = wheres.map(canonical)
  private def canonical(a:Aggregate):Aggregate = a.to(table)
  private def canonical(aggregates:Aggregates):Aggregates = aggregates.map(canonical)
  private def canonical(g:GroupByInterval):GroupByInterval = GroupByInterval(canonical(g.col), g.interval)
  private def canonical(groupByIntervals:GroupByIntervals):GroupByIntervals = groupByIntervals.map(canonical)
  private def canonical(havings:Havings):Havings = havings.map(canonical)
  private def canonical(h:Having):Having = h.to(table)
  private def canonical(o:Ordering):Ordering = Ordering(canonical(o.col), o.isDescending)
  private def canonical(orderings:Orderings):Orderings = orderings.map(canonical)
  private def canonical[T](u:Update[T]):Update[T] = Update(u.col, u.data.to(table).asInstanceOf[T])
  private def canonical[T](updates:Updates[T]):Updates[T] = updates.map(canonical[T])
  
  //////////////////////////////////////////////
  ///// validating methods
  //////////////////////////////////////////////
  private def validateUpdates(updates:Updates[Any]) = {
    updates.foreach{u => u.col.colType match {
        case c:CompositeCol => throw new DBException("update column cannot be composite: "+u.col.colSQLString)
        case _ =>
      }
    }
  }
  private def validateIncrments(cIncrements:Increments) = {
    val incrCols = cIncrements.map(_.col)                                                                                             
    incrCols.foreach{incrCol => incrCol.colType match{ // do not allow increment for composite column
        case CompositeCol(lhs, colOp, rhs) => throw new DBException("increment: composite column not allowed: "+incrCol.colSQLString+" of table: "+incrCol.optTable.getOrElse("None"))
        case _ => // ok
      }
    }
    incrCols.foreach(assertColExists)
    incrCols foreach(f => assert(f.canDoIncrement, "cannot increment col: ["+f+"] in table ["+table+"] of type ["+f.colType+"]"))    
  }
  private def validateAggregates(cAggregates:Aggregates) = {
    cAggregates.flatMap(_.compositeAggregates.map(_.col)) foreach assertColExists                                                     
    checkDupSelectAggr(cAggregates)                                                                                                  
    cAggregates foreach (a => assert(a.canDoAggregate, "cannot aggregate col: "+a.col+":"+a.col.colType))                            
  }
  private def validateWheres(cWheres:Wheres) = {
    val flatWheres = cWheres.flatMap(_.compositeWheres)                                                                              
    flatWheres.foreach{w => 
      assertColExists(w.col)
      w.data match {
        case c:Col => assertColExists(c)
        case _ =>
      }
    }
    flatWheres.foreach(_.checkValidOp)                                                                                               
    checkDupWheres(cWheres)                                                                                                          
  }
  private def validateCols(cCols:Cols) = {
    cCols.foreach(assertColExists)                                                                                                    
    checkDupSelect(cCols)                                                                                                            
  }
  private def validateOrderings(cOrderings:Orderings) = {
    cOrderings map(_.col) foreach assertColExists                                                                                     
  }
  private def validateHavings(cHavings:Havings) = {
    validateAggregates(cHavings.map(_.aggregate))
  }
  private def validateGroupByIntervals(cGroupByIntervals:GroupByIntervals) = {
    cGroupByIntervals.foreach{g => 
      assertColExists(g.col)
      assert(g.col.canDoInterval || g.interval == 0, "interval not permitted: col["+g.col+"] of type["+g.col.colType+"]")      
      // NOTE: g.interval == 0 implies its a GroupBy column
    }
  }
  private def validateCanonical(cCols:Cols, cWheres:Wheres, cOrderings:Orderings, cIncrements:Increments, cAggregates:Aggregates, cGroupByIntervals:GroupByIntervals):Unit = {
    // check all dbs belong to same dbname etc
    validateAggregates(cAggregates)
    validateWheres(cWheres)
    validateIncrments(cIncrements)
    validateCols(cCols)
    validateGroupByIntervals(cGroupByIntervals)
    validateOrderings(cOrderings)
  }
  private def validateCanonical(cCols:Cols, cWheres:Wheres, cOrderings:Orderings, cIncrements:Increments, cAggregates:Aggregates, cGroupByIntervals:GroupByIntervals, cHavings:Havings):Unit = {
    validateCanonical(cCols:Cols, cWheres:Wheres, cOrderings:Orderings, cIncrements:Increments, cAggregates:Aggregates, cGroupByIntervals:GroupByIntervals)
    
  }
  private def checkDupSelect(cols:Cols) = if (cols.toSet.size != cols.size) throw DBException("select cols should not contain duplicates")  
  private def checkDupSelectAggr(aggregates:Aggregates) = {
    // removing the check for now, in secure DB, some aggregates may be duplicated. 
    // For instance, in MOD encryption, AVG maps to (SUM, COUNT).. i.e. one Aggregate can map to two
    // If SUM or COUNT was already there, then this will give an error, as in
    // 
    // db.select(col.sum, col.avg).asLong 
    //  (above will be translated in secure DB to:
    //    db.select(col.sum, col.sum, col.count).asLong)
    //    
    //   This will cause sum to be duplicated
    // 
    //    if (aggregates.toSet.size != aggregates.size) 
    //    throw DBException("aggregate cols should not contain duplicates")
  }
  private def checkDupWheres(wheres:Wheres) = if (wheres.toSet.size != wheres.size) throw DBException("where cols should not contain duplicates")
    
  protected [mux] def setData(data:Array[(Any, DataType)], startCtr:Int, st:PreparedStatement, startDebugString:String) = {
    var debugSqlString = startDebugString
    var setCtr = startCtr
    data.foreach{
      case (data, dataType) =>
        setCtr += 1            
        debugSqlString = debugSqlString.replaceFirst("\\?", data.toString)
        set(setCtr, st, data, dataType)            
    }
    (setCtr, debugSqlString)    
  }
  ////////////////////////////////////////////////////////////////
  //// DML/DDL METHODS START
  ////////////////////////////////////////////////////////////////

   /**
     * Inserts an entry (row) into the table
     *    @param data is an array giving the values to insert in the Cols (columns). Every column must be filled.
     *    @return the number of rows affected by the query
     */ /* to override in SecureDBManager */
  ///////////////////////////////////////////////////////////////////  
  ///////////////////////////////////////////////////////////////////
  // need to fix the Where part to be compatible with joins. dont use ColOp  
  ///////////////////////////////////////////////////////////////////
  ///////////////////////////////////////////////////////////////////
  def insertArray(data:Array[Any]) = {
    assert(data.size == tableCols.size, "schema mismatch table["+table+"]. Expected ["+tableCols.size+"] columns; found ["+data.size+"]")
    using(connection){conn =>
      if (printSQL_?) println("Insert query SQL [?]:\n  "+insertSQLString)
      using(conn.prepareStatement(insertSQLString)){st => {
          val (_, debugInsertString) = setData(data zip tableCols.map(_.colType), 0, st:PreparedStatement, insertSQLString)
          if (printSQL) println("Insert query SQL [R]:\n  "+debugInsertString)           
          st.executeUpdate()
        }
      }
    }
  }  

   /**
     * Updates certain cols (columns) of the rows matching searchCol == searchData.
     *
     *  @param wheres is an Array of Where (the criteria (Col, Op, Value)) for the cols to search in.
     *  @param updates is an array of Update giving the cols and data to update
     *  @return the number of rows updated
     */ /* to override in SecureDBManager */   
  ///////////////////////////////////////////////////////////////////  
  ///////////////////////////////////////////////////////////////////
  // need to fix the Where part to be compatible with joins. dont use ColOp  
  ///////////////////////////////////////////////////////////////////
  ///////////////////////////////////////////////////////////////////
  def updateCols(wheres:Wheres, updates:Updates[Any]):Int = {                   // WHAT IF UPDATES CONTAIN COMPOSITE COLS???
    validateUpdates(updates)
    val (cWheres, cUpdates) = (canonical(wheres), canonical(updates))
    validateCanonical(cUpdates.map(_.col), wheres, Array(), Array(), Array(), Array())
    using(connection){conn =>
      val sqlString = updateSQLString(cWheres, cUpdates)
      if (printSQL_?) println("Update query SQL [?]:\n  "+sqlString)
      using(conn.prepareStatement(sqlString)){st => {
          val (updateSetCtr, debugUpdateString) = setData(cUpdates.map(u => (u.data, u.col.colType))++getWheresData(cWheres), 0, st, sqlString)          
          if (printSQL) println("Update query SQL [R]:\n  "+debugUpdateString)
          st.executeUpdate()
        }
      }
    }
  } 

   /**
     * Updates the table by incrementing integer value cols (updateCols) by the values specified in incrementValues (which can be negative integers)
     * The row(s) matching the wheres will be incremented.
     *
     * This is just a wrapper that reads, adds data and then updates. (the standard way)
     * A more efficient way (read and update in one step) is given in the deprecated @incrementCols
     *
     *  @param wheres is an Array of Where (the criteria (Col, Op, Value)) for the cols to search in.
     *  @param increment is of type Array[Update] representing the columns to be updated along with the data.
     *  For each Update in this array, the cols MUST be INT, UINT, LONG or ULONG types
     *  @return the number of rows updated
     *
     *  The caller should ensure that the colType of each element of updateCols is INT.
     */ /* no need to override in SecureDBManager (it calls other overridden methods) */
  ///////////////////////////////////////////////////////////////////
  // need to fix the Where part to be compatible with joins. dont use ColOp  
  ///////////////////////////////////////////////////////////////////
  def incrementColsTx(wheres:Wheres, increments:Increments):Int = using(connection){conn => // WHAT IF INCREMENTS CONTAIN COMPOSITE COLS???
    //val (cWheres, cIncrements) = (canonical(wheres), canonical(increments))    
    conn.setAutoCommit(false) // transaction start
    try {
      val incrCols = increments.map(_.col)
      val after = selectCols(wheres, incrCols, incrementIt(_, increments.map(_.data)))
      val resCols = after.size match {
        case 0 => throw new DBException("increment: no rows matched in table ["+table+"] for cols ["+incrCols.map(_.name).reduceLeft(_+","+_)+"]")
        case 1 => updateCols(wheres, incrCols zip after(0) map(z => Update(z._1, z._2)))
        case n => throw new DBException("increment: > 1  rows ["+n+"] matched in table ["+table+"] for cols ["+incrCols.map(_.name).reduceLeft(_+","+_)+"]")
      }
      conn.commit // transaction commit if everything goes well
      resCols
    } catch { 
      case e: Exception => 
        e.printStackTrace
        conn.rollback 
        throw new DBException("Increment aborted. Table ["+table+"]. Error: "+e.getMessage)
    } finally {
      conn.setAutoCommit(true)
    }
  }
  
   /**
    * This attempts to provide ACID via the DB's update with x = x + 1 feature (see http://stackoverflow.com/a/15987873/243233http://stackoverflow.com/a/15987873/243233)
    * However, it depend's on the DBMS's check statement to ensure that unsigned numbers never go below 0.
    * 
    *      (Why incrementCols is used instead of a combination of read and then update?)
    *  This is due to concurrency issues and "lost updates". See:
    *    http://dba.fyicenter.com/Interview-Questions/RDBMS-FUNDAMENTALS/RDBMS_FUNDAMENTALS_Lost_Update_Problem.html
    *    http://cisnet.baruch.cuny.edu/holowczak/classes/3400/dbms_functions/
    *
    *  The solution used here (increment the col before read) is based on the suggestion at the following links
    *    http://dev.mysql.com/doc/refman/5.0/en/innodb-locking-reads.html
    *    http://dev.mysql.com/doc/refman/5.1/en/innodb-locking-reads.html
    *  The same links also suggest an alternate fix - to use "SELECT FOR UPDATE". However, in our case, this seems to be the most convenient fix.
    *
    * Note that MySQL does not support check. Hence this is insecure/unreliable in MySQL.
    * For this reason, do not use this at all. Use incrementColTx instead which performs an ACID update (read, increment, update in a transaction)
    * If not using H2 or Postgresql, this is fine to use.
    */ /* to override in SecureDBManager */
  ///////////////////////////////////////////////////////////////////
  // need to fix the Where part to be compatible with joins. dont use ColOp  
  ///////////////////////////////////////////////////////////////////
  @deprecated def incrementCols(wheres:Wheres, increments:Increments) = { /// WHAT IF INCREMENT CONTAINS COMPOSITE COLS???
    val (cWheres, cIncrements) = (canonical(wheres), canonical(increments))
    validateCanonical(Array(), cWheres, Array(), cIncrements, Array(), Array())
    
    using(connection){conn =>
      val sqlString = incrementColsString(cWheres, cIncrements)
      if (printSQL_?) println("IncrementColTx query SQL [?]:\n  "+sqlString)
      using(conn.prepareStatement(sqlString)){st => {
          val (incrementSetCtr, debugIncrementString) = setData(cIncrements.map(u => (u.data, u.col.colType)) ++ getWheresData(cWheres), 0, st, sqlString)          
          if (printSQL) println("IncrementColTx query SQL [R]:\n  "+debugIncrementString)
          st.executeUpdate()
        }
      }
    }
  }

   /**
     * Deletes rows matching the searchCol criteria.
     *
     *  @param where is the criteria (Col, Op, Value) for the col to search in.
     *  @param data is the data (of type Any) to search for in searchCol. The type of the data must match with the colType of searchCol array,
     *  otherwise an exception is thrown (for instance, if searchCol.colType = INT and data is "XYZ")
     *  @return the number of rows deleted
     */ /* to override in SecureDBManager */
  def delete(wheres:Wheres):Int = {
    val cWheres = canonical(wheres)
    validateCanonical(Array(), cWheres, Array(), Array(), Array(), Array())
    using(connection){conn =>
      val sqlString = deleteSQLString(cWheres)
      if (printSQL_?) println("Delete query SQL [?]:\n  "+sqlString)
      using(conn.prepareStatement(sqlString)){st => {
          val (whereSetCtr , whereDebugString) = setData(getWheresData(cWheres), 0, st, sqlString)          
          if (printSQL) println("Delete query SQL [R]:\n  "+whereDebugString)
          val i = st.executeUpdate()
          i
        }
      }
    }
  }
   /** 
    * protected def is used because this will need to be overridden in SecureDBManager 
    * 
    * http://stackoverflow.com/a/197300/243233
    * 
    */ /* to override in SecureDBManager */
  protected[db] def aggregateGroupHaving[T](aggregates:Aggregates, wheres:Wheres, groupByIntervals:GroupByIntervals, havings:Havings, func: (RS, String)=>T)(implicit func2:String => T = ???) = { // default implementation missing. Must provide implicit (or explicit) parameter
    val (cAggregatesTemp, cWheres, cGroupByIntervals, cHavings) = (canonical(aggregates), canonical(wheres), canonical(groupByIntervals), canonical(havings))
    
    // purpose of following block.
    // first note the different types of SQL queries. Assume that users is a Col of type VARCHAR, age and sal of ULONG
    // 
    // 1. SELECT MAX(age) from T where ... 
    // 2. SELECT TOP(age), SUM(sal), 100 * round(age / 100, 0) as foo from T where ... GROUP BY foo  (the 100 * round ... part is internally inserted due to a group-by-interval)
    // 3. SELECT SUM(age), userID as foo from T where ... GROUP BY foo
    // 
    // The first case is for queries of type 3 (containing a column without aggregate -- userID -- that MUST be included in the group-by clause).
    // This check ensures that the MUST is satisfied
    // 
    // The second case is for queries of type 2 (containing a query Top). Top is non-standard SQL and is a syntactic sugar provided by us. It MUST be used in conjunction 
    // with a group-by-interval. This check ensures that the MUST is satisfied.
    // Note that we can specify Top without the interval if only one group-by-interval exists for that column. If multiple exist, then we need to specify the interval in top: as in
    // 
    // db.aggregate(age.top withInterval 20, age.sum).groupByAggregate(age withInterval 20) 
    //  NOTE: above code is equiv to 
    // db.aggregate(age.top \ 20, age.sum).groupByAggregate(age \ 20) 
    // db.aggregate(age.topWithInterval(20), age.sum).groupByAggregate(age \ 20) 
    // 
    val cAggregates = cAggregatesTemp.map{
      case a@Aggregate(col, DataStructures.GroupBy) =>       
        if (cGroupByIntervals.contains(GroupByInterval(col, 0))) a
        else throw DBException(s"""Cannot do GroupBy(${col}) without a Group-by clause for that col""")
      case a@Aggregate(col, Top(Some(interval))) =>       
        if (cGroupByIntervals.contains(GroupByInterval(col, interval))) a
        else throw DBException(s"""Cannot do Top(${col}) with interval ${interval} without a Group-by-interval clause for that (col, interval)""")
      case Aggregate(col, Top) =>
        cGroupByIntervals.groupBy(_.col).get(col) match {
          case Some(groups) if groups.size == 1 => 
            val grp = groups(0)
            Aggregate(col, Top(Some(grp.interval)))
          case Some(groups) => 
            throw DBException(s"""Cannot determine interval for Top(${col}) because more than one Group-by-inverval for that column exists. 
If more than one Group-by-interval is used for a column, then Top for that column MUST use an interval""")
          case _ => 
            throw DBException(s"""Cannot do Top(${col}) because a Group-by-interval clause is not present for that column. 
For making a Top query on a column, that column MUST be used in Group-by-interval.""") 
        }
      case any => any
    }
    
    // SELECT MAX(USERS.SAL) as LjkkudMSTzMAX,((USERS.AGE + USERS.SAL) + 4) as uOzFPSGTyfinterval0 FROM USERS WHERE USERS.SAL > 10000 GROUP BY uOzFPSGTyfinterval0    
    // SELECT max(age + (sal + 3))/avg(sal - 5) group by (sal + 2)
    validateCanonical(Array(), cWheres, Array(), Array(), cAggregates, cGroupByIntervals, cHavings)    
    using(connection){conn => 
      val sqlString = aggregateSQLString(cAggregates, cWheres, cGroupByIntervals, cHavings) // setting var for debug
      if (printSQL_?) println("AggregateGroup query SQL [?]:\n  "+sqlString)
      using(conn.prepareStatement(sqlString)){st => {          
          val (aggrSetCtr, debugAggrString) = setData(
            getAggregateData(cAggregates, cGroupByIntervals, cWheres, cHavings),
            0, st:PreparedStatement, sqlString
          )          
          if (printSQL) println("AggregateGroup query SQL [R]:\n  "+debugAggrString)
          using(st.executeQuery) { rs => 
            bmap(rs.next){ 
              cAggregates.map(ag => {
                  ag.aggr match {
                    case Last | First =>
                      val x = rs.getArray(ag.alias).getArray.asInstanceOf[Array[Object]](0).asInstanceOf[String].split(',')
                      func2((if (ag.aggr == First) x.head else x.last))
                    case Top(Some(interval)) => func(rs, GroupByInterval(ag.col, interval).alias)
                    case DataStructures.GroupBy => //func(rs, GroupByInterval(ag.col, 0).alias)
                      get(rs, ag.col, Some(GroupByInterval(ag.col, 0).alias))
                    case Top => throw DBException("Top must specify an interval that was used in Group-by-interval for that column") // func(rs, GroupByInterval(ag.col, interval).alias)                    
                    case any => 
                      func(rs, ag.alias)
                  }
                }
              )
            } 
          } 
        }
      }
    }
  }

  def selectInto(db:DBManager, cWheres:Wheres, cCols:Cols)(implicit cOrderings:Orderings=Array(), limit:Int = 0, offset:Long = 0) = {
    // query of type INSERT INTO T1 SELECT A, B, C FROM T2
    selectAnyResultSet(
      (cCols, cWheres, cOrderings, limit, offset) => insertIntoSQLString(db.getTable, cCols, cWheres)(cOrderings, limit, offset),
      _.executeUpdate,
      cWheres:Wheres, cCols:Cols
    )
  }
  
  private def selectResultSet[T](cWheres:Wheres, cCols:Cols, func:ResultSet => T) (implicit cOrderings:Orderings=Array(), limit:Int = 0, offset:Long = 0):T = {
    selectAnyResultSet(
      (cCols, cWheres, cOrderings, limit, offset) => {
        selectSQLString(cCols, cWheres)(cOrderings, limit, offset)
      },
      st => using (st.executeQuery){func},
      cWheres:Wheres, cCols:Cols
    )
  }
  private def selectAnyResultSet[T](
    getSQLString:(Cols, Wheres, Orderings, Int, Long) => String,
    doSQLQuery:PreparedStatement => T,
    cWheres:Wheres, cCols:Cols) (implicit cOrderings:Orderings=Array(), limit:Int = 0, offset:Long = 0):T = {
    using(connection){conn =>
      val sqlString = getSQLString(cCols, cWheres, cOrderings, limit, offset)
      if (printSQL_?) println("Select query SQL [?]:\n  "+sqlString)
      using(conn.prepareStatement(sqlString)){st => {
          val (_, debugSelectString) = setData(getSelectData(cCols:Cols, cWheres:Wheres, cOrderings:Orderings), 0, st, sqlString)
          if (printSQL) println("Select query SQL [R]:\n  "+debugSelectString)
          doSQLQuery(st)//{func}
        }
      }
    }
  }

  protected [db] def getWheresData(cWheres:Wheres):Array[(Any, DataType)] = cWheres.flatMap(_.nestedWhereData)++cWheres.flatMap(_.compositeWheresData)
  
  protected [db] def getHavingsData(cHavings:Havings) = cHavings.flatMap(_.nestedHavingData) ++ cHavings.flatMap(_.compositeHavingsData)
  
  protected [db] def getSelectData(cCols:Cols, cWheres:Wheres, cOrderings:Orderings):Array[(Any, DataType)] = 
    cCols.flatMap{a => a.compositeColData} ++ getWheresData(cWheres)++ cOrderings.flatMap{_.col.compositeColData}
  
  protected [db] def getAggregateData(cAggregates:Aggregates, cGroupByIntervals:GroupByIntervals, cWheres:Wheres, cHavings:Havings):Array[(Any, DataType)] = 
    cAggregates.flatMap(_.compositeAggrData) ++ cGroupByIntervals.flatMap(_.col.compositeColData) ++ getWheresData(cWheres) ++ getHavingsData(cHavings) 
  
  // will override this in SecureDBManager (done?)
  
   /**
     *
     * Searches table and returns a list of objects of generic type T matching the colToSearch criteria.
     *
     * Some code from http://www.roseindia.net/jdbc/Jdbc-functions.shtml
     *
     *  @param T is a custom data type (defined using a class) such as Person, Item, etc.
     *  @param wheres is an Array[Where] (the criteria for the cols to search in).
     *  @param select is an Array[Col] representing one or more cols (columns) that we want to extract from the select query
     *  @param func is a function that maps any array (i.e., Array[Any]) to type T. The input to the function will be the array
     *  containing the data for the cols contained in selectCols. The output can be any type the user wants.  The subtype of the elements
     *  of the input array is determined by the type of cols contained in where:
     *    INT ---> Int ; VARCHAR ---> String ; VARBINARY ---> Array[Byte] ; TIMESTAMP ---> String.
     *
     *  For instance:
     *  If searchCols is Array(RealValueCol, ImaginaryValueCol), where RealValueCol and ImaginaryValueCol are of type INT each, then
     *  the input to the function is an Array[Any] containing two Int elements. The function can, for instance, map this array as follows
     *  def func(ar:Array[Any]) = new ComplexNumber(ar(0).asInstanceOf[Int], ar(0).asIntanceOf[Int])
     *
     *  @return a list of type T
     *
     */  /* to override in SecureDBManager */
  // search M select M  
  def selectCols[T](wheres:Wheres, cols:Cols, func: Array[Any]=>T) (implicit orderings:Orderings=Array(), limit:Int = 0, offset:Long = 0):List[T] = {
    val (cWheres, cCols, cOrderings) = (canonical(wheres), canonical(cols), canonical(orderings))    
    validateCanonical(cCols, cWheres, cOrderings, Array(), Array(), Array())
    selectResultSet(cWheres, cCols, rs => bmap(rs.next)(func (cCols.map(get(rs, _)))))(cOrderings, limit, offset)
    
    // why not use below? (implicit limit and offset are automatically supplied?)
    // selectResultSet(cWheres, cSelectCols, rs => bmap(rs.next)(func (cSelectCols.map(get(rs, _)))))(cOrderings)
  }
  // will override this in SecureDBManager (toDo)
  def selectRS[T](wheres:Wheres, cols:Cols, func: ResultSet => T) (implicit orderings:Orderings=Array(), limit:Int = 0, offset:Long = 0):T = {
    val (cWheres, cCols, cOrderings) = (canonical(wheres), canonical(cols), canonical(orderings))
    selectResultSet(cWheres, cCols, func)(cOrderings, limit, offset)  
  }
  // will override this in SecureDBManager (toDo) // commented below because moved to DBManager 
  //  def insertRS[T](cols:Cols, rs:ResultSet) = bmap(rs.next)(insert(cols.map(get(rs, _)))).size   

  /**
     * counts all rows in the table matching the select criteria given by wheres
     *  @param wheres is an Array of Where (the criteria (Col, Op, Value)) for the cols to search in.
     *
     */  /* to override in SecureDBManager */  
  def countRows(wheres:Wheres):Long = {
    val cWheres = canonical(wheres)
    validateCanonical(Array(), cWheres, Array(), Array(), Array(), Array())      
    using(connection){conn =>
      val sqlString = countSQLString(cWheres)
      if (printSQL_?) println("Count query SQL [?]:\n  "+sqlString)
      using(conn.prepareStatement(sqlString)){st => {
          val (_, whereDebugString) = setData(getWheresData(cWheres), 0, st, sqlString)          
          if (printSQL) println("Count query SQL [R]:\n  "+whereDebugString)           
          using (st.executeQuery){rs =>
            rs.next
            rs.getLong(1)
          }
        }
      }
    }
  }
}

//////////////////////////////////////////////
//////////////////////////////////////////////
//////////////////////////////////////////////
//////////////////////////////////////////////
//////////////////////////////////////////////
//////////////////////////////////////////////
//////////////////////////////////////////////








