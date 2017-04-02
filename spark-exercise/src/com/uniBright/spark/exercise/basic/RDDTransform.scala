package com.uniBright.spark.exercise.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object RDDTransform {
  def main(args : Array[String] ){
    val conf = new SparkConf().setAppName("Transform testing")
    val sc = new SparkContext(conf)
    val data = sc.textFile("/home/roger/test/1.txt")
    var mapresult = data.map(line => line.split("\\s+"))
    mapresult.collect
    
    var flatmapresult = data.flatMap(line => line.split("\\s+"))
    flatmapresult.collect
    data.map(_.toUpperCase).collect
    data.flatMap(_.toUpperCase).collect
    
    data.map(x => x.split("\\s+")).collect
    data.flatMap(x => x.split("\\s+")).collect
    
    data.flatMap(line => line.split("\\s+")).collect
    data.flatMap(line => line.split("\\s+")).distinct.collect
  }
}