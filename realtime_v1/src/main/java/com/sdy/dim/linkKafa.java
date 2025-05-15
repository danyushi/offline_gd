package com.sdy.dim;

import com.sdy.utils.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.sdy.dwd.linkKafa
 * @Author danyu-shi
 * @Date 2025/5/15 16:19
 * @description:
 */
public class linkKafa {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> kafkauserbase2Ds = KafkaUtil.getKafkaSource(env, "realtime_v2_result_label_user_base2", "group_log");

//        kafkauserbase2Ds.print();
        DataStreamSource<String> kafkauserbase4Ds = KafkaUtil.getKafkaSource(env, "realtime_v2_result_label_user_base4", "group_log");


        DataStreamSource<String> kafkauserbase6Ds = KafkaUtil.getKafkaSource(env, "realtime_v2_result_label_user_base6", "group_log");


        DataStream<String> mergedStream = kafkauserbase2Ds
                .union(kafkauserbase4Ds)
                .union(kafkauserbase6Ds);

        mergedStream.print();



        env.execute();
    }
}
