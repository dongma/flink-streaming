package org.streaming.flow.util

import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.streaming.flow.SensorReading

import scala.util.Random

/**
 * Flink Source函数，用随机温度来生成SensorReading数据流
 *
 * @author Sam Ma
 * @date 2022/11/08
 */
class SensorSource extends RichParallelSourceFunction[SensorReading] {

  // 标识whether#source仍然在运行中
  var running: Boolean = true

  /**
   * long running job, 会持续通过SourceContext emits SensorReadings
   *
   * @param srcCtx
   */
  override def run(srcCtx: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()
    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask
    // 初始化censorId和temperature温度
    var curFTemp = (1 to 10).map {
      i => ("sensor_" + (taskIdx * 10 + i), 65 + (Random.nextGaussian() * 20))
    }

    while (running) {
      curFTemp = curFTemp.map(t => (t._1, t._2 + (rand.nextGaussian() * 0.5)))
      val curTime = Calendar.getInstance.getTimeInMillis
      // 向flink中提交新的SensorReading，提交完一次后并sleep 100ms
      curFTemp.foreach(t => srcCtx.collect(SensorReading(t._1, curTime, t._2)))

      Thread.sleep(100)
    }
  }

  /** cancel()方法用来取消SourceFunction的执行 */
  override def cancel(): Unit = {
    running = false
  }

}
