package amit.common.file.prop

import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.util.Properties
import amit.common.file.Util
import amit.common.file.Util._
import amit.common.Util._

trait TraitCommonFilePropReader {  
  val propertyFile : String
  val propertyDirectory = "properties"  
  def fileExists = Util.fileExists(fullFileName)
  private def actualFileName = propertyFile
  def fullFileName : String = propertyDirectory + "/" +actualFileName   
  override def toString = fullFileName
  var isInitialized = false
  val props = new Properties
  final val readOnlyTag = "$readOnly"
  lazy val readOnly = read(readOnlyTag, false)
  
  // the following processes the inputstream
  def processIS(is:InputStream):InputStream  
  def initialize = { // for command-line and web-server
    def stream1 = () => new FileInputStream(fullFileName)
    def stream2 = () => this.getClass().getClassLoader().getResourceAsStream(fullFileName)
    def stream3 = () => new FileInputStream("conf/"+actualFileName) // for play framework
    try {
      using(trycatch(List(stream1, stream2, stream3))){is =>
        props.load(processIS(is))
        println("Loaded propertyfile ["+propertyFile+"] from: "+is)
      }
    } catch { case e : Throwable => 
        println ("File not found: "+fullFileName)        
    }
    isInitialized = true
  }
  protected def read[T](name:String, default: T, func:String=>T): T = {
    if (!isInitialized) initialize
    val p = props.getProperty(name)
    if (p == null) {
      println("property ["+fullFileName+":"+name+"] not found. Using default value of "+default)
      default
    } else func(p.trim)
  }
  protected def read(name:String, default: String):String = read[String](name, default, x => x)
  protected def read(name:String, default: Int):Int = read[Int](name, default, _.toInt)
  protected def read(name:String, default: Long):Long= read[Long](name, default, _.toLong)
  protected def read(name:String, default: Double):Double= read[Double](name, default, _.toDouble)

  protected def read(name:String):String = read(name, "")
  protected def read(name:String, default: Boolean):Boolean = read[Boolean](name, default, _.toBoolean)
  protected def processOS(os:OutputStream):OutputStream
  def getBackUpFile = fullFileName+"_"+toDateString(getTime).replace(":", "-").replace(" ", "_")+".bak"
  def write(name:String, value:String, comment:String, backupFile:Boolean=false) = if (!readOnly) {
    val backedupFile = if (backupFile && fileExists) backup else "None"
    props.setProperty(name, value)
    props.store(processOS(new FileOutputStream(fullFileName)), comment)    
    backedupFile
  } else throw new Exception("File is readonly due to "+readOnlyTag+" tag: "+fullFileName)
  def backup = copyTo(getBackUpFile)
  def copyTo(fileName:String) = {
    if (Util.fileExists(fileName)) throw new Exception("file already exists: "+fileName)
    props.store(processOS(new FileOutputStream(fileName)), "backup")    
    fileName
  }
}








