import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util
import scala.Console.println
import scala.jdk.CollectionConverters.*
import com.typesafe.config.Config
import com.typesafe.config.*
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

import scala.runtime.Nothing$


// LongWritable: is a Writable class that wraps a java long
// Text: stores text using standard UTF8 encoding
// IntWritable: is a Writable class for ints
// MapReduceBase: (no-op = no operation)

// this will grab the ocnfigurations from application.conf
var conf: Config = ConfigFactory.load("application.conf")

// val ListOfLine: List[String] = List()
var ListOfLine = new ListBuffer[String]()

var count = 1 // will be used to select the important information
var fillerOut = 0 // will be used to ignore the first two rows of information that is not needed


object MapReduceProgram:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()

    // OutputCollector: Collects the <key, value> pairs output by the Mappers and Reducers
    // Reporter to report progress and update counters, status information
    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      // This is where it splits the text apart
      // time,context name, message level, logging module, message

      // this will hold the values of each line of log
      // The list will filter and then consist of [Tme, message Level, Message]

      if(fillerOut > 1)
        line.split(" ").foreach { token =>
          word.set(token)
          output.collect(word, one)
          // println(token)
          if(token != "log")
            if(count == 1 || count == 3 || count == 7)
              ListOfLine += token
              println(token + " Number: " + count)
            if (token == "DEBUG" || token == "ERROR")
              count += 1
            if(count == 7) {
              count = 0
            }

            count += 1
            fillerOut = -1
        }
      fillerOut += 1
      // val temp = conf.getInt("group.split")



  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val timer = conf.getInt("predefined.TimeInterval")

      // cur to keep track of our intervals
      var cur: Long = 0
      // to know our placement in the time format
      var count: Int = 1
      // splits the time so I can convert to milliseoncds
      ListOfLine(0).split("[:,.]").foreach { time =>
        // for hours
        if(count == 1) {
          cur = time.toInt * 3600000
        }
        // for minutes
        if(count == 2) {
          cur += time.toInt * 60000
        }
        // for seconds
        if(count == 3) {
          cur += time.toInt * 1000
        }
        // for millliseconds
        if(count > 3) {
          cur += time.toInt
        }
        count += 1
      }
      //keep track of where the numbers are at
      var index: Int = 0
      val mLevel = scala.collection.mutable.Map()

      ListOfLine.foreach { value =>
        // means we have hit the time format
        if(index % 3 == 0) {
          // we go beyond the timer we are in a new interval
          if(toMS(index) - cur <= timer)  {

          } else {
            cur = toMS(index) // a new interval
          }
          println(toMS(index))
        }

        index += 1
      }
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key,  new IntWritable(sum.get()))

  // this will help to calculate the time to ms
  def toMS(index : Int) : Long = {
    // cur to keep track of our intervals
    var cur: Long = 0
    // to know our placement in the time format
    var count: Int = 1
    // splits the time so I can convert to milliseoncds
    ListOfLine(index).split("[:,.]").foreach { time =>
      // for hours
      if (count == 1) {
        cur = time.toInt * 3600000
      }
      // for minutes
      if (count == 2) {
        cur += time.toInt * 60000
      }
      // for seconds
      if (count == 3) {
        cur += time.toInt * 1000
      }
      // for millliseconds
      if (count > 3) {
        cur += time.toInt
      }
      count += 1
    }
    return cur
  }

  @main def runMapReduce(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("Parse Message")
    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)

