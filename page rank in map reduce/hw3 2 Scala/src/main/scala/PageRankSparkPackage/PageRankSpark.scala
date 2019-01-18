package PageRankSparkPackage

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


object PageRankSpark {

  def getAdjacencyListData(line : Iterable[(String,Int)]): ArrayBuffer[(String,String)] = {
    var adList = ArrayBuffer[(String,String)]()
    for(i <- line){
      var toString = (i._1,i._2.toString)
      adList += toString
    }
    adList
  }


  def extractVertices(item : (String,(ArrayBuffer[(String,String)] , Int))): ArrayBuffer[(String,Int)]  ={
    val infi = 109820
    println("\n\n\n\n\n\n\n\n\n\n\n")
    println(item)
    var ls = ArrayBuffer[(String,Int)]()
    var selfNode = (item._1,item._2._2)
    ls +=selfNode

    for(n <- item._2._1){
      var node = n._1.toString
      var weight = 0
      if (item._2._2.toInt >= infi){
        weight = infi
      }
      else {
        weight = n._2.toInt + item._2._2.toInt
      }
      var pair =(node, weight)
      ls += pair
//      (n._1.toString, n._2.toInt + item._2.toInt)
    }
    return ls
  }

  def min(x: Int, y: Int): Int = {
    if (x.toInt>y.toInt){
      return y
    }
    return x
  }

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nPageRankSparkpackage.PageRankSpark <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("PageRankSpark")
    val sc = new SparkContext(conf)

    //Define the source
    val infi = 109820     //needs to be defined very high. for now i have defined is to this as i am using a small data set

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    val textFile = sc.textFile(args(0))


    //getting the input and making the adjaceny list
    var twitterfollowers = textFile.map(line => (line.split(",")(0), ((line.split(",")(1),1))))
        .groupByKey()

    val sourceNode ="1"

    //making the graph
    val graph = twitterfollowers.map(line => (line._1,getAdjacencyListData(line._2)))
    graph.persist()

    // extract the source as this is for single source path
    var sourceAdjList = graph.filter(item => item._1 == sourceNode).first()._2
    var sourceAdjListNode = sourceAdjList.map(item => item._1)
    var distinctNodes = graph.flatMap(item => (item._2))

    println("\n\n\n\n\n\n\n\n\n\n\n")
    println(sourceAdjList)
    println("\n\n\n\n\n\n\n\n\n\n\n")

    //make the initial distance from the source.
    var distances = distinctNodes.map( n =>
      if(sourceAdjListNode.contains(n._1)){

        var ns = sourceAdjList.filter(item => n._1 == item._1).head
        (n._1,ns._2.toInt)
      }
      else{
        (n._1, infi)
      }).distinct()

    var i = 0

    //the stopping condition i am using is to check if any of the vales are still infi
    // as the distance between nodes is equal we can use this condition to check
    // and also check if the values remained unchecked as we can start at a random node
    var stopCondition = false
    if (distances.filter(item => item._2 >= infi).first() != null) {
     stopCondition = true
    }

    while (stopCondition) {
      distances = graph.join(distances)
        .flatMap(item => extractVertices(item))
        .reduceByKey((x, y) => min(x, y))
      var filteredRDD = distances.filter(item => item._2 >= infi)
      // check for the stopping condition
      if (!filteredRDD.isEmpty()) {
        stopCondition = true
      }
      else {
        stopCondition = false
      }
    }

    println("source node")
    println()
    println("longest path found")
    println(distances.reduce((x, y) => if(x._2 > y._2) x else y))

    distances.saveAsTextFile(args(1))
  }
}
