package com.tv.wc

import org.apache.flink.api.scala._

// 批处理的wordcount
object WordCount {
  def main(args: Array[String]): Unit ={
    // 创建一个批处理执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件读取数据, 需要是绝对路径
    val inputPath = "/Users/allen/bigdataapp/flinktutorial/src/main/resources/hello.txt"
    val inputDataSet = env.readTextFile(inputPath)

    // 分词之后做 count 处理
    val wordCountDataSet = inputDataSet
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    // 打印输出
    wordCountDataSet.print()

  }
}
