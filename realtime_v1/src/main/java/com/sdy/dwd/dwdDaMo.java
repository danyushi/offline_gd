package com.sdy.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.sdy.domain.Constant;
import com.sdy.utils.KafkaUtils;

import java.time.Duration;
import java.util.Date;

/**
 * @Package com.sdy.dwd.dwdDaMo
 * @Author danyu-shi
 * @Date 2025/5/12 15:09
 * @description:
 */
public class dwdDaMo {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        Constant.KAFKA_BROKERS,
                        Constant.TOPIC_DB,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> JSONObject.parseObject(event).getLong("ts_ms")),
                "kafka_cdc_db_source"
        ).uid("kafka_cdc_db_source").name("kafka_cdc_db_source");


//        kafkaCdcDbSource.print();

        DataStream<JSONObject> filteredOrderInfoStream = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(json -> json.getJSONObject("source").getString("table").equals("order_info"))
                .uid("kafka_cdc_db_order_source").name("kafka_cdc_db_order_source");
//        filteredOrderInfoStream.print("order_info----->");


        DataStream<JSONObject> filteredOrderDetailStream = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(json -> json.getJSONObject("source").getString("table").equals("order_detail"))
                .uid("kafka_cdc_db_order_detail_source").name("kafka_cdc_db_order_detail_source");
//                filteredOrderDetailStream.print("order_detail------>");


        DataStream<JSONObject> filteredUserStream = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(json -> json.getJSONObject("source").getString("table").equals("comment_info"))
                .uid("kafka_cdc_db_user_source").name("kafka_cdc_db_user_source");
//                filteredUserStream.print("comment_info------>");

        DataStream<JSONObject> filteredUserStream2 = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(json -> json.getJSONObject("source").getString("table").equals("user_info_sup_msg"))
                .uid("kafka_cdc_db_user_source2").name("kafka_cdc_db_user_source2");
        filteredUserStream2.print("user_info_sup_msg------>");





        env.execute();

    }
}
