package com.werxltd

import junit.framework._
import org.junit.Test
import org.junit.Before
import Assert._
import org.mockito.Mockito._

import org.slf4j.{Logger, LoggerFactory}
import org.apache.commons.io.FileUtils
import java.io.File
import java.util.ArrayList

import org.apache.hadoop.io.{LongWritable, Text}

object RegexMRTest {
  def suite: TestSuite = {
    val suite = new TestSuite(classOf[RegexMRTest]);
    suite
  }

  def main(args : Array[String]) {
    junit.textui.TestRunner.run(suite);
  }
}

class RegexMRTest extends TestCase("regex") {
  val logger: Logger = LoggerFactory.getLogger(getClass)
  
  var test_strings:List[String] = null
  
  @Before
  override def setUp:Unit = {
    test_strings = scala.io.Source.fromFile(inputfile).getLines.toList
  }
  
  val regexfile = "src/test/resources/input/regexes"
  val inputfile = "src/test/resources/input/sample"
  val outputdir = "src/test/resources/output"
    
  @Test
  def testMapper1 = {
    val mapper = new RegexMRMapper()
    mapper.regexes = List(".*")
    
    val value = new Text(test_strings(0))
    val output = mock(classOf[RegexMRMapper#Context])
    
    mapper.map(null, value, output)
    verify(output).write(new Text(".*"), new LongWritable(1))
  }
  
  @Test
  def testMapper2 = {
    val mapper = new RegexMRMapper()
    mapper.regexes = List(".*(?i)PAYMODE.*")
    
    val value = new Text(test_strings(0))
    val output = mock(classOf[RegexMRMapper#Context])
    
    mapper.map(null, value, output)
    verify(output).write(new Text(".*(?i)PAYMODE.*"), new LongWritable(1))
  }
  
  // Make sure 1+1 still equals 2
  @Test
  def testWordCountReducer = {
    val reducer = new RegexMRReducer()
    val output = mock(classOf[RegexMRReducer#Context])
    val key = new Text("Mcafee")
    val values = new ArrayList[LongWritable]()
    values.add(new LongWritable(1))
    values.add(new LongWritable(1))
    reducer.reduce(key, values, output)
    verify(output).write(key, new LongWritable(2))
  }
  
//  @TODO Broken, needs to be fixed
//  @Test
//  def testWholeJob = {
//    FileUtils.deleteDirectory(new File(outputdir))
//
//    RegexMR.main(Array(regexfile,inputfile,outputdir))
//
//    val result = scala.io.Source.fromFile(outputdir + "/part-r-00000").getLines.toList
//  
//    println("result: "+result)
//    
//    assert(result.contains(".* 5"))
//          
//    FileUtils.deleteDirectory(new File(outputdir))
//  }
}
