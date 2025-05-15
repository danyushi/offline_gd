package com.sdy.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sdy.domain.Constant;
import com.sdy.func.ProcessJoinBase2And4BaseFunc;
import com.sdy.func.ProcessLabelFunc;
import com.sdy.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.phoenix.util.CSVCommonsLoader;


import java.time.Duration;
import java.util.Date;

/**
 * @Package com.label.DbusUserLabel6BaseCalculate
 * @Author danyu_shi
 * @Date 2025/5/15 15:32
 * @description:
 */
public class DbusUserLabel6BaseCalculate {
    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<String> kafkaBase6Source = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                Constant.KAFKA_BROKERS,
                                Constant.kafka_label_base6_topic,
                                new Date().toString(),
                                OffsetsInitializer.earliest()
                        ),
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> {
                                            JSONObject jsonObject = JSONObject.parseObject(event);
                                            if (event != null && jsonObject.containsKey("ts_ms")){
                                                try {
                                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                                }catch (Exception e){
                                                    e.printStackTrace();
                                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                                    return 0L;
                                                }
                                            }
                                            return 0L;
                                        }
                                ),
                        "kafka_label_base6_topic_source"
                ).uid("kafka_base6_source")
                .name("kafka_base6_source");

        SingleOutputStreamOperator<String> kafkaBase4Source = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                Constant.KAFKA_BROKERS,
                                Constant.kafka_label_base4_topic,
                                new Date().toString(),
                                OffsetsInitializer.earliest()
                        ),
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> {
                                            JSONObject jsonObject = JSONObject.parseObject(event);
                                            if (event != null && jsonObject.containsKey("ts_ms")){
                                                try {
                                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                                }catch (Exception e){
                                                    e.printStackTrace();
                                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                                    return 0L;
                                                }
                                            }
                                            return 0L;
                                        }
                                ),
                        "kafka_label_base4_topic_source"
                ).uid("kafka_base4_source")
                .name("kafka_base4_source");

        SingleOutputStreamOperator<String> kafkaBase2Source = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                Constant.KAFKA_BROKERS,
                                Constant.kafka_label_base2_topic,
                                new Date().toString(),
                                OffsetsInitializer.earliest()
                        ),
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> {
                                            JSONObject jsonObject = JSONObject.parseObject(event);
                                            if (event != null && jsonObject.containsKey("ts_ms")){
                                                try {
                                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                                }catch (Exception e){
                                                    e.printStackTrace();
                                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                                    return 0L;
                                                }
                                            }
                                            return 0L;
                                        }
                                ),
                        "kafka_label_base2_topic_source"
                ).uid("kafka_base2_source")
                .name("kafka_base2_source");

        SingleOutputStreamOperator<JSONObject> mapBase6LabelDs = kafkaBase6Source.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> mapBase4LabelDs = kafkaBase4Source.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> mapBase2LabelDs = kafkaBase2Source.map(JSON::parseObject);


        SingleOutputStreamOperator<JSONObject> join2_4Ds = mapBase2LabelDs.keyBy(o -> o.getString("uid"))
                .intervalJoin(mapBase4LabelDs.keyBy(o -> o.getString("uid")))
                .between(Time.days(-24), Time.days(24))
                .process(new ProcessJoinBase2And4BaseFunc());

        SingleOutputStreamOperator<JSONObject> waterJoin2_4 = join2_4Ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (jsonObject, l) -> jsonObject.getLongValue("ts_ms")));

        SingleOutputStreamOperator<JSONObject> userLabelProcessDs = waterJoin2_4.keyBy(o -> o.getString("uid"))
                .intervalJoin(mapBase6LabelDs.keyBy(o -> o.getString("uid")))
                .between(Time.days(-24), Time.days(24))
                .process(new ProcessLabelFunc());


        userLabelProcessDs.print();

        env.execute();
    }

}
