package org.streaming.flow.util

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.streaming.flow.SensorReading

/**
 * 给SensorReading分配Timestamps，基于其内部的时间戳，并提交watermark
 *
 * @author Sam Ma
 * @date 2022/11/08
 */
class SensorTimeAssigner extends
  BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {

  /** 从sensorReading提取出timestamp数据 */
  override def extractTimestamp(t: SensorReading): Long = {
    t.timestamp
  }

}
