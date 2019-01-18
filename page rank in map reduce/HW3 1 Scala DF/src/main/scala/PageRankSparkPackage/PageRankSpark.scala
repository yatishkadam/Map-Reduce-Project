package PageRankSparkPackage

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext, sql}

import scala.collection.mutable.ArrayBuffer


object PageRankSpark {

  //Function which returns a pair
  // (1,2)
    //if its the end node it will return
    //(endNode,0)
  def makePairs(s : String): (String, String) = {
    if (s.split(",").length > 1){
      val val1 = s.split(",")(0)
      val val2 = s.split(",")(1)
      //      print (val1,val2)
      return (val1,val2)
    }
    else{
      return (s.split(",")(0), "0")
    }

  }


  //This is used to make the individual nodes which are needed to hold the initial page ranks
  def individualNodes(s : Int) : String = {
    var ls = StringBuilder.newBuilder
    var i = 0
    for (i <- 1 to (s*s)){
      ls.append(i.toString)
      ls.append('|')
    }
    return ls.toString()
  }

  //this function is used to make all the possible pairs for the given k
  //given a k it returns  a array buffer containing the nodes.
  def makeNodes(k : Int) : ArrayBuffer[(String,String)] = {
    var ls = ArrayBuffer[(String,String)]()
    var temp =1
    var i = 0
    var j = 0
    for (i <- 1 to k){
      for (j <- 1 to k){
        var s = StringBuilder.newBuilder
        if (temp%k != 0){
          s.append(temp.toString)
          s.append(',')
          s.append((temp+1).toString)
          ls+=makePairs(s.toString())
        }
        else
        {
          ls+=makePairs(temp.toString)
        }
        temp+=1
      }
    }
    //    print(ls.toString())
    return ls
  }


  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nPageRankSparkpackage.PageRankSpark <input dir> <output dir>")
      System.exit(1)
    }
    val t1 = System.nanoTime
    val conf = new SparkConf().setAppName("PageRankSpark")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sqlContext = new sql.SQLContext(sc)
    import sqlContext.implicits._
    val k = 100

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    //initial distinct nodes RDD
    val distinctGraphNodes = sc.parallelize(individualNodes(k).split('|').map(item => item),2)

    //initial page rank
    val initialPageRank = 1.0/ (k*k)

    //The graph nodes RDD
    // is of the form
    // (source, destination) pairs
    val graphs = sc.parallelize(makeNodes(k).map(item => (item._1,item._2)), 2).persist()
    //Converting to a DataFrame
    var dataFrame = graphs.toDF("source","dest").persist()
    //initial page rank
    var pageRank = distinctGraphNodes.map(node => (node.toString,initialPageRank)).toDF("source", "pr")

    //this is used to loop for a certain number of iterations
    var i = 0
    for (i <- 1 to 10) {
      //intial join fucntion to get the (v2,pr)
      var join1 = dataFrame.join(pageRank, Seq("source"), "left").select("dest", "pr")
        .groupBy('dest).sum("pr")
        .select($"dest" as "dest", $"sum(pr)" as "pr")
//      join1.show()

        //get value for 0
      var deltaVal = join1.filter(join1("dest").equalTo("0"))
        .select("pr").collectAsList().get(0).getDouble(0) / k
      // we remve the node 0
      join1 = join1.filter(join1("dest").notEqual("0"))
      //Second join to introduce the missing pages back into the loop
      pageRank = pageRank.join(join1, pageRank("source") === join1("dest"), "left_outer")
        .select(pageRank("source"), join1("pr"))
        .na.fill(0.0, Seq("pr"))   //used to give the missing nodes a value of 0.0
      pageRank = pageRank.withColumn("pr", pageRank("pr") + deltaVal)
    }

    pageRank.orderBy($"pr".desc)
    pageRank.show(100)
    println("Time Duration")
    val duration = (System.nanoTime - t1) / 1e9d
    println(duration)
    println()
  }
}
