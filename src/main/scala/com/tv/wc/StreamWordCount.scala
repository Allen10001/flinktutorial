package com.tv.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * P7 flink 流式处理
  */
object StreamWordCount {
  def main(args: Array[String]): Unit ={

    val params = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    // 创建一个流式处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val textDataSteam : DataStream[String] = env.socketTextStream(host,port)

    // 逐一读取数据，打散之后进行 word count
    val wordCountDataStream = textDataSteam.flatMap(_.split("\\s"))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1)

    // 打印输出
    wordCountDataStream.print().setParallelism(3)

    // 执行任务
    env.execute("stream word count job")
  }

}
