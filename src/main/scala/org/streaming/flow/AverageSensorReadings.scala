package org.streaming.flow

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.streaming.flow.util.{SensorSource, SensorTimeAssigner}

/**
 * 对传感器中的数据流，每5秒计算一次平均温度
 *
 * @author Sam Ma
 * @date 2022/11/08
 */
object AverageSensorReadings {

  def main(args: Array[String]): Unit = {
    // 设置流式应用的执行环境，并在应用中使用事件流 TimeCharacteristic#EventTime, 竟被Deprecated
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sensorData: DataStream[SensorReading] = env
      // 从流式数据源中创建DataStream[SensorReading]对象，利用SensorSource Function获取传感器读数
      .addSource(new SensorSource)
      // 分配时间戳和水位线
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

  }

}
