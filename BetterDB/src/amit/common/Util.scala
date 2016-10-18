package amit.common

import amit.common.encoding.Base64
import amit.common.file._
import java.io.BufferedReader
import java.io.InputStreamReader
import java.sql.Date
import java.text.SimpleDateFormat
import scala.collection.mutable.{Set => MSet}
import scala.concurrent.duration._

import akka.actor.ActorSystem
object TaskScheduler {
  val actorSystem = ActorSystem()
  val scheduler = actorSystem.scheduler
  val task = new Runnable { def run() { println("Hello") } }
  implicit val executor = actorSystem.dispatcher
  def doOnce(fn: => Unit, period:Long) = scheduler.scheduleOnce(period milliseconds)(fn) 
  def doRegularly(fn: => Unit, periodMillis:Long) = scheduler.schedule(0 seconds, periodMillis milliseconds)(fn)
  def doWhile(fn: => Unit, whileFn: => Boolean, period:Long) {
    if (whileFn) {
      fn
      doOnce(doWhile(fn, whileFn, period), period)
    }
  } 
  def doWhileTail(fn: => Unit, whileFn: => Boolean, period:Long) =
    doOnce(doWhile(fn, whileFn, period), period)
  
  def doWhileTailOld(fn: => Unit, whileFn: => Boolean, period:Long) {
    doOnce(if (whileFn){
             fn
             doWhileTailOld(fn, whileFn, period)
           }, period)
  } 
}

object UtilConfig extends TraitPlaintextFileProperties {
  val propertyFile = "AmitCommon.properties"
  var debug = read("debug", false)
}
object Util {
  def rand = scala.util.Random.nextInt.abs
  def debug = UtilConfig.debug
  def addUnique[T](id:Int, l:MSet[(Int, T)], t:T) = if (!l.exists(x => x._1 == id)) l += ((id, t))
  val TenMins = 1000L*60*10
  val ThirtyMins = 1000L*60*30
  val FifteenMins = 1000L*60*15
  val FiveMins = 1000L*60*5
  val OneMin = 1000L*60
  val OneHour = 1000L*60*60
  def printdbg(s:String) = if (debug) println(s)
  def getTime = System.currentTimeMillis
  def doWhile(fn: => Unit, whileFn: => Boolean, period:Long) = TaskScheduler.doWhile(fn, whileFn, period)
  def doWhileTail(fn: => Unit, whileFn: => Boolean, period:Long) = TaskScheduler.doWhileTail(fn, whileFn, period)
  def doOnce(fn: => Unit, periodMillis:Long) = TaskScheduler.doOnce(fn, periodMillis)
  def doOnceNow(fn: => Unit) = doOnce(fn, 0)
  def doRegularly(fn: => Unit, periodMillis:Long) = TaskScheduler.doRegularly(fn, periodMillis)
  def doHourly(f: => Unit) = doRegularly(f, 1000L*60*60)  // every hour
  def doEvery30Mins(f: => Unit) = doRegularly(f, 1000L*60*30)  // every 1/2 hr
  def doEvery15Mins(f: => Unit) = doRegularly(f, 1000L*60*15)  // every 15 mins
  def doEvery10Mins(f: => Unit) = doRegularly(f, 1000L*60*10)  // every 10 mins
  def doEvery5Mins(f: => Unit) = doRegularly(f, 1000L*60*5)  // every 5 mins
  def doEveryMin(f: => Unit) = doRegularly(f, 1000L*60)  // every 1 min
  def toTimeString(millis:Long) = {
    val (second, minute, hour) = ((millis / 1000) % 60, (millis / (1000 * 60)) % 60, (millis / (1000 * 60 * 60)) % 24)
    "%02d:%02d:%02d:%d".format(hour, minute, second, millis % 1000)
  }
  def toDateString(millis:Long) = new SimpleDateFormat("yyyy MMM dd HH:mm:ss").format(new Date(millis))  

  val d = java.security.MessageDigest.getInstance("SHA-256");
  final val englishAlphaBet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
  def sha256Bytes(b:Array[Byte]):String = Base64.encodeBytes(d.digest(b))
  def sha256(s:String):String = sha256Bytes(s.getBytes)
  def shaSmall(s:String):String = (cleanString(sha256(s))  match { // removes non-alphanumeric chars such as / and = 
    case x if "0123456789".contains(x(0)) => englishAlphaBet(x(0).toString.toInt)+x.substring(1)
    case x => x
  }).substring(0, 16)
  def cleanString(s:String) = if (s == null) "" else s.replaceAll("[^\\p{L}\\p{Nd}]", "") // removes non-alphanumeric chars
  val random = new scala.util.Random(new java.security.SecureRandom()) 
  
  def randomString(alphabet: String)(n: Int): String = // Generate a random string of length n from the given alphabet
    Stream.continually(random.nextInt(alphabet.size)).map(alphabet).take(n).mkString
  def randomAlphanumericString(n: Int) =     // Generate a random alphabnumeric string of length n
    randomString(englishAlphaBet)(n)
  def isFromAlphabet(alphaBet:String)(toCheck:String) = toCheck forall (alphaBet.contains(_))
  def isNonAlphaNumeric(s:String) = s.matches("^.*[^a-zA-Z0-9 ].*$");
  def stopAll(param: {def stop: Unit}*) = param.foreach(x => x.stop)
  def extractStrings(s:String):Array[String] = if (s != null) s.split(':').map(_.trim) else Array()
  def getIntList (t:Traversable[_]) = List.range(0, t.size)

  def round(d:Double, decimals:Int) = 
    BigDecimal(d).setScale(decimals, BigDecimal.RoundingMode.HALF_UP)
  def round(s:String, decimals:Int) = 
    BigDecimal(s).setScale(decimals, BigDecimal.RoundingMode.HALF_UP).toString
  
  def compareBytes(a:Array[Byte], b:Array[Byte]) =
    if (a.length == b.length) { // length must be equal
      var res = true
      Util.getIntList(a).foreach (i => res = res && a.apply(i)== b.apply(i))
      // above line computes the AND of the comparison of individual bytes in this and a
      res
    } else false

  def readUserInput(prompt:String) = {
    val br = new BufferedReader(new InputStreamReader(System.in))
    print (prompt)
    br.readLine()
  }
  /**
   * Generic function
   * it invokes func and if an exception, returns the result of func2, otherwise it returns the result of func. Both methods output generic type T 
   */
  def getOrElse[T](func : => T, func2: => T) =
    getOrNone(func).getOrElse(func2)


  /**
   * Generic function
   * Takes as input func and func1, which are two methods. Both methods output generic type T and take no input.
   * This method invokes func and if an exception occurs, it returns None, otherwise it returns the result of func enclosed in a "Some" object. Therefore this method returns Option[T]
   */
  def getOrNone[T](func : => T) = {
    try{
      Some(func)
    } catch {
      case _ : Throwable => None
    }
  }
  def tryIt(f: => Any) = try f catch { case t:Throwable => if (debug) t.printStackTrace else println("Error:"+t.getMessage)}
  def getOrElseExit[T](func:()=>T) = try { func.apply } catch { case e:Exception =>
      println (e.getMessage+"\nProgram will exit")
      if (debug) e.printStackTrace else println("Error:"+e.getMessage)
      System.exit(1)
  }

  /**
   * Used for closing DB connections implicitly.
   * Also used for writing / reading to files
   * Code is borrowed - need to check correctness.
   * @author From the book "Beginning Scala"
   */
  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
  try { f(param) } finally { param.close() }

  /**
   * Takes in a list of methods mapping from Unit to B. 
   * Tries the first method, and if exception, tries the second (otherwise returns), and so on until no exception or no more methods.
   * Returns the output of the final method called either B or output of last method (which can be B or an exception).
   * 
   */
  type unitToT[T] = ()=>T
  def trycatch[B](list:List[unitToT[B]]):B = list.size match {
    case i if i > 1 => 
      try {
        list.head()
      } catch {
        case t:Any => trycatch(list.tail)
      }
    case 1 => 
      if (debug) println("trycatch with one element")
      list(0)()
    case _ => throw new Exception("call list must be non-empty")
  }


  /** used for DB read */
  import scala.collection.mutable.ListBuffer
  def bmap[T](test: => Boolean)(block: => T): List[T] = {
    val ret = new ListBuffer[T]
    while(test) ret += block
    ret.toList
  }
}


