package mux.db.core.DataStructures


import mux.db.DBManager
import mux.db.core.Table
  
/**
 * Scala type corresponding to a SQL Col type. A SQL Col has a name and a SQL data type
 */

object Col {
  def apply(colName:String, colType:DataType) = new Col(colName, colType, None)
  // following from http://www.h2database.com/html/advanced.html
  val reservedNames = Array("CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DISTINCT", "EXCEPT", "EXISTS", "FALSE", "FETCH", "FOR", "FROM", "FULL", "GROUP", "HAVING", "INNER", "INTERSECT", "IS", "JOIN", "LIKE", "LIMIT", "MINUS", "NATURAL", "NOT", "NULL", "OFFSET", "ON", "ORDER", "PRIMARY", "ROWNUM", "SELECT", "SYSDATE", "SYSTIME", "SYSTIMESTAMP", "TODAY", "TRUE", "UNION", "UNIQUE", "WHERE")
}
case class Col(name:String, colType:DataType, optTable:Option[Table]) {
  if (Col.reservedNames.contains(name.toUpperCase)) throw DBException(s"column name $name is a reserved word")
  lazy val compositeColData:List[(Any, DataType)] = { // gives data for a composite col // need to check ordring of data 
    colType match {
      case CompositeCol(lhs, oper, rhs) => 
        lhs.compositeColData ++ (
          rhs match {
            case c:Col => c.compositeColData 
            case data => List((data, lhs.colType)) 
          }
        ) 
      case CONST(any, t) => List((any, t))
      case _ => Nil        
    }
  }
  lazy val isComposite = colType match {
    case CompositeCol(lhs, oper, rhs) => true
    case _ => false
  }
  lazy val colSQLString:String = {
    colType match {
      case CompositeCol(lhs, oper, rhs) => 
        "("+lhs.colSQLString + " "+
        oper + " "+
        rhs.anySQLString+")" // in Where, lhs and Eq are not required for computing the output..
        // (but given because Where needs them in constructor, however getDataString does not use these values, if you look at its code)
      case CONST(any, _) => 
        any.anySQLString
      case other => 
        (if (optTable.isDefined) optTable.get.tableName+"." else "")+name
    }
  }

  lazy val compositeTables:Set[Table] =  { // in the query Select A+B from T, (A+B) is a composite column
    colType match{
      case CompositeCol(lhs, oper, rhs:Col) => lhs.compositeTables ++ rhs.compositeTables
      case CompositeCol(lhs, oper, rhs) => lhs.compositeTables
      //case _ => if (optTable.isDefined) Set(optTable.get.tableName) else Set()
      case _ => if (optTable.isDefined) Set(optTable.get) else Set()
    }
  }

  lazy val compositeTableNames = {
    ////////////////////////////////////////////////
    //////// ORDERING DOES NOT MATTER AS OUTPUT IS SET
    ////////////////////////////////////////////////
    compositeTables.map(_.tableName)
  }

  lazy val compositeCols:Set[Col] = colType match { // get accessed cols
    ////////////////////////////////////////////////
    //////// ORDERING DOES NOT MATTER AS OUTPUT IS SET
    ////////////////////////////////////////////////
    case CompositeCol(lhs, oper, rhs:Col) => lhs.compositeCols ++ rhs.compositeCols
    case CompositeCol(lhs, oper, any) => lhs.compositeCols
    case any => Set(this)
  }

  lazy val simpleCol:Col = Col(name, colType match{case CompositeCol(lhs, _, _) => lhs.simpleCol.colType case any => colType}, None)

  lazy val colHash = compositeColData.foldLeft(hash(colSQLString))((x, y) => hash(x+y._1))
  lazy val alias = colHash // hash(tableColName)

  lazy val canDoInterval = colType match { // for group by interval // no big Int
    case INT | LONG | UINT(_) | ULONG(_) | CompositeCol(_,_,_) => true                  // should we allow interval for composite cols?
    case _ => false
  }
  @deprecated lazy val canDoIncrement = canDoInterval

  override def toString = compositeColData.foldLeft(colSQLString)((x, y) => x.replaceFirst("\\?", y._1.toString))

  // BELOW METHOD converts all optTable in this column to Some(table) (including those of composite cols contained within this col) if optTable is undefined, otherwise leaves unchanged
  def to(table:Table):Col = {
    val newColType = colType match {
      case CompositeCol(lhs, oper, rhs) => 
        val newLhs = lhs.to(table)
        val newRhs = rhs match {
          case c@Col(_, _, _) => c.to(table)
          case any => any
        }
        CompositeCol(newLhs, oper, newRhs)
      case any => any
    }
    Col(name, newColType, if (optTable.isDefined) optTable else Some(table))
  }  

  // changes the optTable of current col to Some(table). Also converts tables of composite cols to Some(table) IF those optTables are not defined (else leaves unchanged)
  // however, it DOES change for current col (irrespect of what optTable is). The unchanging behaviour is only for composite cols
  def of(table:Table):Col = Col(name, colType, Some(table)).to(table)
  def of(db:DBManager):Col = of(db.getTable) // for DBManager, we call getTable rather than table so that we can override it in SecureDBManager
        // why? because we need to get the plaintext table, not ciphertext table when defining of using SecureDBManager (super class of DBManager)

  // helper methods for composite cols
  def +(rhs:Any)    = Col(name, CompositeCol(this, Add, rhs), optTable) // keeping name and optTable same for now
  def -(rhs:Any)    = Col(name, CompositeCol(this, Sub, rhs), optTable) // keeping name and optTable same for now
  def /(rhs:Any)    = Col(name, CompositeCol(this, Div, rhs), optTable) // keeping name and optTable same for now
  def *(rhs:Any)    = Col(name, CompositeCol(this, Mul, rhs), optTable) // keeping name and optTable same for now
  def %(rhs:Any)    = Col(name, CompositeCol(this, Mod, rhs), optTable) // keeping name and optTable same for now
  def \(interval:Long)    = GroupByInterval(this, interval)
  def withInterval(interval:Long) = \(interval)

  def decreasing = Ordering(this, Decreasing)

  // helper methods for wheres

  // following three lines based on this answer 
  // http://stackoverflow.com/a/34809016/243233
  private var x0: Where = _
  def value = x0
  def value_=(data: Any) = Where(this, Eq, data)  

  def === (rhs:Any)    = Where(this, Eq, rhs)
  def from (rhs:Nested)    = Where(this, From, rhs)
  
  def in(strings:Array[String]) = Where(this, In, strings)
  def in (rhs:Nested)    = Where(this, In, rhs)
  def notIn (rhs:Nested)    = Where(this, NotIn, rhs)
  
  def <=(rhs:Any)    = Where(this, Le, rhs)
  def >=(rhs:Any)    = Where(this, Ge, rhs)
  def <(rhs:Any)    = Where(this, Lt, rhs)
  def >(rhs:Any)    = Where(this, Gt, rhs)
  def <>(rhs:Any)    = Where(this, Ne, rhs)
  def like(rhs:Any)    = Where(this, Like, rhs)
  def notLike(rhs:Any)    = Where(this, NotLike, rhs)
  def ~(rhs:Any)    = Where(this, RegExp, rhs)
  def !~(rhs:Any)    = Where(this, NotRegExp, rhs)

  def sum = Aggregate(this, Sum)
  def max = Aggregate(this, Max)
  def min = Aggregate(this, Min)
  def avg = Aggregate(this, Avg)
  def first = Aggregate(this, First)
  def last = Aggregate(this, Last)
  def count = Aggregate(this, Count)
  def top = Aggregate(this, Top)
  def groupBy = Aggregate(this, GroupBy)
  def topWithInterval(interval:Long) = Aggregate(this, Top(Some(interval)))

  def <--(data:Any) = Update(this, data)
  def ++=(data:Number):Increment = Update(this, data)
}
