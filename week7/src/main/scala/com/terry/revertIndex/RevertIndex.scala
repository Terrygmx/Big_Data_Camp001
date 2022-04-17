package com.terry.revertIndex

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File


object RevertIndex {
	def main(args: Array[String]): Unit = {
		if (args.length<2) {
			Console.err.println("please enter input path, output path")
			return
		}
		val appName = "InvertIndex"
		val master = "local[*]"
		val conf = new SparkConf().setAppName(appName).setMaster(master)
		val sc= new SparkContext(conf)
		sc.setLogLevel("WARN")
		val direct = new File(args(0))
		var allKvRDD = sc.emptyRDD[(String, List[(String,Integer)])]
		val fileList = direct.listFiles()
		for (f<-fileList){
			val path = f.toString
//			println("filePath is:",path)
			val arr = path.split("/")
			val fn = arr(arr.length-1)
			// 以行为单位做分词
			val lineRDD: RDD[String] = sc.textFile("file://"+path)
			val wordRDD: RDD[String] = lineRDD.flatMap(line => line.split(" "))
			val cleanWordRDD: RDD[String] = wordRDD.filter(word => !word.equals(""))
			// 把RDD元素转换为（Key，Value）的形式
			val cntKvRDD: RDD[(String, Integer)] = cleanWordRDD.map(word => (word,1))
			// 按照单词做累加
			val wordCountRDD: RDD[(String, Integer)] = cntKvRDD.reduceByKey((x, y) => x + y)
			// wordCountRDD 元素转换为 a: {(2,1)} 的形式
			val transKvRDD: RDD[(String, List[(String,Integer)])] = wordCountRDD.mapValues(cnt => List((fn,cnt)))
			allKvRDD=allKvRDD.union(transKvRDD)
		}

		// 把来自各单词的全部结果合并
		val result: RDD[(String, List[(String,Integer)])] = allKvRDD.reduceByKey((x, y) => x ++ y)
		// 收集结果
		result.saveAsTextFile(args(1))
	}
}
