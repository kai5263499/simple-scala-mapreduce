package com.werxltd

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Reducer, Job, Mapper}
import org.apache.hadoop.conf.{Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.filecache.DistributedCache
import scala.collection.mutable.HashSet
import java.io.BufferedReader
import java.io.FileReader
import java.net.URI
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConversions._
import org.apache.hadoop.util.{ToolRunner, Tool}

object RegexMR extends Configured with Tool with HImplicits {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def run(args: Array[String]): Int = {
    logger.debug("regexfile: [{}] input: [{}] outpath: [{}]",args(0),args(1),args(2))
    
    val conf = getConf
    conf.setQuietMode(false)

    DistributedCache.addCacheFile(new URI(args(0)), conf)
    DistributedCache.createSymlink(conf);
    
    val job: Job = new Job(conf, "Repper Regex Tester")

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[RegexMRMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[LongWritable])
    
    job.setCombinerClass(classOf[RegexMRReducer])
    job.setReducerClass(classOf[RegexMRReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[LongWritable])

    FileInputFormat.addInputPath(job, args.apply(1))
    FileOutputFormat.setOutputPath(job, args.apply(2))

    job.waitForCompletion(true) match {
      case true => 0
      case false => 1
    }
  }

  def main(args: Array[String]) {
    System.exit(ToolRunner.run(this, args))
  }

}

class RegexMRMapper extends Mapper[LongWritable, Text, Text, LongWritable] with HImplicits {
  val logger: Logger = LoggerFactory.getLogger(getClass)
  
  val url_pattern = """.* url(cat){0,1}:([^ ,]*),.* .*""".r
  var regexes:List[String] = null
  
  override def setup(context: Mapper[LongWritable, Text, Text, LongWritable]#Context): Unit = {
   
    val uris = DistributedCache.getCacheFiles(context.getConfiguration())
    
    if(logger.isDebugEnabled()) logger.debug("uris", uris)
    
    regexes = io.Source.fromFile(uris.head.toString(),"utf-8").getLines.toList
  }
  
  override def map(lnNumber: LongWritable, line: Text, context: Mapper[LongWritable, Text, Text, LongWritable]#Context): Unit = {
    if(!line.contains("url")) return
    logger.debug("processing line {}",line)
    
    url_pattern.findAllIn(line).matchData foreach {
       m => {
         logger.debug("groups 0:[{}] 1:[{}] 2:[{}]",m.group(0),m.group(1),m.group(2))
         val url_str:String = new String(new sun.misc.BASE64Decoder().decodeBuffer(m.group(2)))
         for (regex:String <- regexes) {
            logger.debug("processing regex {}",regex)
            if(url_str.matches(regex)) {
              logger.debug("matched regex {} to str {}{}",regex, url_str,"")
              context.write(regex, 1)
            }
         }
       }
    }    
  }
}

class RegexMRReducer extends Reducer[Text, LongWritable, Text, LongWritable] with HImplicits {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  override def reduce(key: Text, value: java.lang.Iterable[LongWritable], context: Reducer[Text, LongWritable, Text, LongWritable]#Context): Unit = {
    logger.debug("reducing {}", key)
    context.write(key, value.reduceLeft(_ + _))
    //OR context.write(token, counts.foldLeft(0)((res, curr) => res + curr.toInt))
  }
}

