package org.streaming.flowdata.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 从文件中读取word单词，并按key进行group然后进行count累加
 *
 * @author Sam Ma
 * @date 2022/11/09
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.readTextFile("the_path_for_input");
        DataStream<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer())
                .keyBy(0).sum(1);
        counts.writeAsText("the_path_for_input");

        env.execute("Streaming WordCount");
    }

    /**
     * Tokenizer实现FlatMapFunction方法，flatMap方法可用于将英文段落按分割符进行分割，
     *  生成Token[]数组
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 使用正则表达式，按"\\W+"对字符串进行分割
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

}
