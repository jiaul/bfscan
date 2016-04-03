/* k-mean clustering in spark */
package io.bfscan.util


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

object kmeanSpark {
  def main(args: Array[String]) {
  
	if (args.length < 4) {
		System.err.println("Usage: <document vector path> <# cluster> <# iterations> <output file name>")
		System.exit(-1)
	}

	val conf = new SparkConf().setAppName("k-means clustering")
	val sc = new SparkContext(conf)
	
	val docFile = args(0) 
	val numClusters = args(1).toInt
	val numIterations = args(2).toInt
	val outFile = args(3)
	val centerFile = outFile + ".centers"
	val assignFile = outFile + ".assign"
	
	val documents = sc.textFile(docFile)
	val parsedData = documents.map(s => {
	 val arr = s.split(" ", 2);
	 new Tuple2(arr(0), 
	   Vectors.dense(arr(1).split(" ").map(_.toDouble)))
	})
	
	val clusters = KMeans.train(parsedData.map(_._2), numClusters, numIterations)

	val clusterCenters = clusters.clusterCenters
	val clusterCentersRDD = sc.parallelize(clusterCenters)
	clusterCentersRDD.saveAsTextFile(centerFile)

	val docAssigned = parsedData.map(s => new Tuple2(s._1, clusters.predict(s._2)))
	docAssigned.saveAsTextFile(assignFile)
  }
}
