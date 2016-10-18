package mux.db.core

import DataStructures._
import amit.common.file.TraitFilePropertyReader
import amit.common.json.JSONUtil.JsonFormatted
import java.sql.SQLException
import java.sql.Timestamp
import java.util.Date
import mux.db.DBManager
import org.bouncycastle.util.encoders.Base64
/** Util is used for translating between SQL data types and Scala data types. The following rules are used
 *
 *    SQL Col element type                        corresponding Scala subtype (of Any)
 *    ----------------------------------------------------------------------------------
 *    INT                                           Int
 *    BIGINT                                        Long
 *    BIGINT UNSIGNED                               ULong
 *    VARCHAR                                       String
 *    VARBINARY                                     Array[Byte]
 *    TIMESTAMP                                     String
 *    BLOB                                          Array[Byte]
 *
 */
object Util extends TraitFilePropertyReader {
  val propertyFile = "commonDB.properties" // common to all DBManagers ...
  var printSQL = read("printSQL", false)
  var printSQL_? = read("printSQL_?", false) // prints ? instead of variables (parametrized SQL)
  
  import java.sql.{ResultSet, PreparedStatement}
  def getTableIDs(tableInfo:Map[String, DBManager]):Seq[JsonFormatted] = getTableIDs(tableInfo.toArray: _*)
  def getTableIDs(tableInfo:(String, DBManager)*) = tableInfo.map{
    case (info, db) => new JsonFormatted{
      val keys = Array("info", "id", "tableName")
      val vals = Array[Any](info, db.getTableID, db.getTable.tableName)
    }
  }
  def getRows(cols:Cols, datas:List[List[Any]]) = {
    datas map {case d => getRow(cols, d)}
  }
  def castData(d:DataType, data:String): Any = {
    // for user-defined where (via UI) for searching and deleting
    d match{
      case TIMESTAMP => data.toLong
      case VARBINARY(_) | BLOB=> Base64.decode(data)
      case ScalaBIGINT(size) => BigInt(data) 
      case UScalaBIGINT(size) => BigInt(data)
      case VARCHAR(_)|VARCHARANY => data
      case INT | UINT(_) => data.toInt
      case LONG | ULONG(_) => data.toLong
      //case BLOB => rs.getBlob
      case any => throw new DBException("unsupported type: "+any)
    }
  }
  def getRow(cols:Cols, data:Array[Any]):Row = getRow(cols, data.toList)
  def getRow(cols:Cols, data:List[Any]) = {
    val names = cols.map(_.name.toLowerCase)
    val newData = data.map { d =>
      d match {
        case a:Array[Byte] => new String(a, "UTF-8")
        case any => any
      }
    }
    Row(newData, names)
  }
  /**
   * This function maps Scala data types to a string description of the corresponding SQL data types.
   * This is used for generating SQL statements.
   */
  def colTypeString(t:DataType) = t match {
    case ScalaBIGINT(size) => "VARCHAR("+size+")"
    case UScalaBIGINT(size) => "VARCHAR("+size+")"
    case VARCHAR(size) => "VARCHAR("+size+")"
    case VARCHARANY => "VARCHAR"
    case VARBINARY(size) => "VARBINARY("+size+")"
    case TIMESTAMP => "TIMESTAMP"
    case INT => "INT"
    case UINT(_) => "INT"
    case LONG => "BIGINT"
    case ULONG(_) => "BIGINT"
    case BLOB => "BLOB"
    case CompositeCol(_, _, _)|CONST(_, _) => throw new DBException("unsupported colType: "+t)
  }
  /**
   *
   * Used to extract a particular Scala Col from a SQL query result set.
   *
   * @return The data stored in ResultSet rs corresponding to Col col
   *
   * @param rs The ResultSet to use
   * @param col The Col to extract from rs. Returns () if col does not exist in rs.
   */
  def get(rs:ResultSet, col:Col, optAlias:Option[String]=None) = {
    /**
     *
     * A function that takes in a Scala DataType (say t) and returns another function f as follows:
     *  f takes in as input a string corresponding to a SQL Col name (say n) and extracts the value of Scala type corresponding to t for col d from 
     *  the result set, if the col with name n is indeed of type t, otherwise it outputs Unit.
     *
     *  So for instance if t is INT, then f takes any string corresponding to a col name (such as "AGE") and outputs the corresponding
     *  Scala type equivalent to INT from the result set (if "AGE" is indeed of type INT).
     *
     *  This method is used only internally by the other methods and should never need be used externally.
     *
     * @param d is a DataType (e.g., VARBINARY)
     * @return A function that takes in a string representing a col name (e.g., "MyColumn") and outputs a Any type
     * depending on d. For instance, if d is VARBINARY, the function will return rs.getBytes
     *
     */
    def getFunc(d:DataType):String=> Any = {
      d match{
        //case TIMESTAMP => rs.getString  // timestamp read as a string. Might change to timestamp type in future
        case TIMESTAMP => rs.getTimestamp // timestamp read as a string. Might change to timestamp type in future
        case VARBINARY(s) => rs.getBytes
        case BOOLEAN => 
          rs.getInt(_) match {
            case 0 => false 
            case _ => true
          }          
        case ScalaBIGINT(size) => s => BigInt(rs.getString(s)) // scala bigint stored as varchar // keep size to 255 usually
        case UScalaBIGINT(size) => s => BigInt(rs.getString(s)) // scala bigint stored as varchar // keep size to 255 usually
        case VARCHAR(_)|VARCHARANY => rs.getString
        case INT | UINT(_) => rs.getInt
        case LONG | ULONG(_) => rs.getLong
        case BLOB => rs.getBytes
        case CompositeCol(lhs, _, _) => getFunc(lhs.colType)
        case CONST(_, t) => getFunc(t)
        //case BLOB => rs.getBlob
        case any => _ => throw new DBException("unsupported type: "+any)
      }
    }
    getFunc(col.colType)(if (optAlias.isDefined) optAlias.get else col.alias)
  }
  def anyException(src:Any, dest:Class[_]) = 
    throw new DBException("I don't know how to convert ["+src+"] of type ["+src.getClass.getCanonicalName+"] to ["+dest.getCanonicalName+"]")

  /**
     * inserts a value into a Prepared Statement in place of "?".
     *
     * @param ctr The "?" is specified by ctr. The value ctr = 1 inserts at the first occurance, ctr = 2 inserts at the second occurance, and so on.
     * @param st The Prepared Statement to insert into
     * @param data the value to insert
     * @param d the datatype of the value
     */
  /*  
    In the method "set" below:
    the "ignoreUnsigned" flag is given to indicate that we can ignore contraint violation on unsigned numbers
    for instance a UINT type is assigned -1
    We can ignore these when the set method is called as part of constructing a "Where" clause, since this is only
    used for searching and not modifying the data. This integrity cannot be violated

    on the other hand, if this method is called as part of a "SET" clause in an "UPDATE" statement, then we do need
    the constraints to be checked. This flag will be then set to false.
  */
  def set(ctr: Int, st:PreparedStatement, data: Any, d:DataType, ignoreUnsigned:Boolean = true):Unit = {
    //println("START ==> "+ctr+"\n "+ st +"\n   DATA: "+data + "\n   TYPE: "+ d)
    def signException(x:Number, d:DataType) = if (!ignoreUnsigned) throw new SQLException("unsigned constraint violation: ["+x+"] for ["+d+"]")
    def rangeException(n:Number, x:Any, d:DataType) = throw new SQLException("range constraint violation: ["+x+"] for ["+d+"] (limit: "+n+")")
    d match {
      case TIMESTAMP  =>  data match { case x: java.sql.Timestamp => st.setTimestamp(ctr, x) }
      case VARBINARY(s) => data match { case x: Array[Byte] => st.setBytes(ctr, x) }
      case VARCHAR(s)   =>  data match { 
          case x: String => 
            if (x.size > s) throw new SQLException("varchar size > "+s+": "+x.take(10)+"...")
            st.setString(ctr, x) 
          case any => 
            println("any "+any.getClass.getCanonicalName)
            anyException(any, classOf[String])
        }
      case VARCHARANY => data match {
          case x:String => st.setString(ctr, x) 
          case any => anyException(any, classOf[String])
        }
      case ScalaBIGINT(s) => data match { 
          case x: BigInt => 
            val str = x.toString // no need to format. Negative numbers do not allow lexicographic ordering.
            // only UScalaBigInt allows ordering, which is formated later
            if (str.size > s) throw new SQLException("scala bigint size > "+s+": "+x.toString.take(10)+"...")
            st.setString(ctr, str) 
          case x: Long => set(ctr, st, BigInt(x), d)
          case x: Int => set(ctr, st, BigInt(x), d)
          case any => anyException(any, classOf[BigInt])
        }
      case INT => data match { 
          case x: Int => st.setInt(ctr, x) 
          case any => anyException(any, classOf[Int])
        }
      case LONG => data match { 
          case x: Long => st.setLong(ctr, x) 
          case x: Int => st.setLong(ctr, x) 
          case any => anyException(any, classOf[Long])
        }
      case UScalaBIGINT(s) => data match { 
          case x: BigInt => // ignoreUnsigned is used to avoid errors when data is in where clause. See top of method definition
            if (x < 0) signException(x, d)
            val str = ("%0"+s+"d").format(x)
            if (str.size > s) rangeException(s, x.toString.take(10)+"...", d)
            //if (str.size > s) throw new SQLException("scala bigint size > "+s+": "+x.toString.take(10)+"...")
            st.setString(ctr, str) 
          case x: Long => set(ctr, st, BigInt(x), d)
          case x: Int => set(ctr, st, BigInt(x), d)
          case any => anyException(any, classOf[BigInt])
        }
      case UINT(n) => data match { 
          case x: Int => 
            if (x < 0) signException(x, d)
            if (x > n) rangeException(n, x, d)
            st.setInt(ctr, x)
          case x:Boolean if n == 1 => st.setInt(ctr, if (x) 1 else 0)
          case any => anyException(any, classOf[Int])
        }
      case ULONG(n) => data match { 
          case x: Long => 
            if (x < 0) signException(x, d)
            if (x > n) rangeException(n, x, d)
            st.setLong(ctr, x) 
          case x: Int => st.setLong(ctr, x) 
          case any => anyException(any, classOf[Long])
        }
      // following code from http://objectmix.com/jdbc-java/41011-create-java-sql-blob-byte%5B%5D.html
      //case BLOB => data match { case x: Array[Byte] => st.setBinaryStream(ctr, new ByteArrayInputStream(x), x.length ) }
      //
      // following code from http://www.herongyang.com/JDBC/SQL-Server-CLOB-setBytes.html
      case BLOB => data match { case x: Array[Byte] => st.setBytes(ctr, x) }
      case CompositeCol(_, oper, _) => 
        (oper, data) match { // used for composite cols.. see method tableColType below
          case (Add| Sub| Mul| Div | Mod, a:Int) => st.setInt(ctr, a) // should we check for division by zero here and below?? I guess not
          case (Add| Sub| Mul| Div | Mod, a:Long) => st.setLong(ctr, a)
          case (Add| Sub| Mul| Div, a:Double) => st.setDouble(ctr, a)
          case (Add| Sub| Mul| Div, a:Float) => st.setFloat(ctr, a)
          case _ => throw new DBException("Data type not supported: "+data+" for oper: "+oper)
        }
      case _ => 
        
    }
  }
  def getSQLTimeStamp:java.sql.Timestamp = { new java.sql.Timestamp(new java.util.Date().getTime) }
  def isTypeMatch(data:Any, colType:DataType) = (data, colType) match { // only checks type matching, doesnt check crypto compatibility (e.g., salts)
    //    cases to handle: cc = composite col; c = ordinary col
    // LHS    RHS
    // c      d
    // cc     c
    //        cc
    // -----------------
    // c      d       OK
    // c      c       OK
    // c      cc      OK
    // cc     d       OK
    // cc     c       OK
    // cc     cc      OK
    case (i:Int, INT|UINT(_)) => true
    case (l:Long, LONG|ULONG(_)) => true
    case (i:Int, LONG|ULONG(_)) => true
    case (i:java.lang.Integer, LONG|ULONG(_)) => true
    case (i:java.lang.Integer, INT|UINT(_)) => true
    case (i:Int, ScalaBIGINT(_)|UScalaBIGINT(_)) => true
    case (l:Long, ScalaBIGINT(_)|UScalaBIGINT(_)) => true
    case (b:BigInt, ScalaBIGINT(_)|UScalaBIGINT(_)) => true
    case (s:String, VARCHAR(_)|VARCHARANY) => true
    case (a:Array[Byte], BLOB|VARBINARY(_)) => true
    case (t:Timestamp, TIMESTAMP) => true
    case (Col(_, LONG|ULONG(_)|INT|UINT(_), _), CompositeCol(_, _, _)) => true  // cc   c
    case (c:Number, CompositeCol(_, _, _)) => true                              // cc   d
    case (c:Col, t) if c.colType == t => true                                   // cc   cc
                                                                                // c    c
    case (c:Col, LONG|ULONG(_)|INT|UINT(_)) if c.isComposite => true            // c    cc
    case _ => false
  }
  def canDoComparison(col:Col, op:Op) = (col.colType, op) match {
    case (colType, _) if colType.isSortable => true
    case (_, From | In | NotIn) => true
    case (CompositeCol(_, _, _), _) => true //  
    case (_, Eq | Ne) => true
    case (VARCHAR(_), Like | NotLike | RegExp | NotRegExp) => true
    case _ => false
  }
  
  // do double check below
  def canDoComparison(aggregate:Aggregate, op:Op) = (aggregate, op) match {
    case (Aggregate(_, Top | First | Last), _)  => false
    case (_, Eq | Ne | Le | Ge | Gt | Lt)  => true
    case (_, From | In | NotIn) => true
    case _ => false
  }
  def incrementIt(a:Array[Any], incrVals:Array[Any]):Array[Any] = (a zip incrVals) map {
    case (a:Int, i:Int) => a+i
    case (a:Long, i:Long) => a+i
    case (a:Long, i:Int) => a+i
    case (a:BigInt, i:BigInt) => a+i
    case (a:BigInt, i:Long) => a+i
    case (a:BigInt, i:Int) => a+i
    case (a, i) => throw new DBException("cannot do increment on ["+a+":+"+a.getClass.getCanonicalName+"] with ["+i+":"+i.getClass.getCanonicalName+"]")
  }  
}
//////////////////////////////////////////
//////////////////////////////////////////
//////////////////////////////////////////
//////////////////////////////////////////
//////////////////////////////////////////
//////////////////////////////////////////
//////////////////////////////////////////
//////////////////////////////////////////
//////////////////////////////////////////
//////////////////////////////////////////
//////////////////////////////////////////
//////////////////////////////////////////
//////////////////////////////////////////