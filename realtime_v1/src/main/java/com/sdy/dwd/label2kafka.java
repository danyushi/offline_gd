package com.sdy.dwd;
import java.time.LocalDate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sdy.bean.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.sdy.retail_v1.realtime.damoplate.userlabel2kafka
 * @Author shidanyu
 * @Date 2025/5/12 11:17
 * @description:
 */
public class label2kafka {

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "xinyi_jiao_yw", "label2kafka");

        // 转json
        SingleOutputStreamOperator<JSONObject> dbJsonDS1 = kafkaSource
                .map(JSONObject::parseObject)
                .filter(json->json.getJSONObject("source").getString("table").equals("user_info"));

        // 转json
        SingleOutputStreamOperator<JSONObject> dbJsonDS2 = kafkaSource
                .map(JSONObject::parseObject)
                .filter(json->json.getJSONObject("source").getString("table").equals("user_info_sup_msg"));

        // 水位线
        SingleOutputStreamOperator<JSONObject> userInfoOutputDS = assignTimestampsAndWatermarks(dbJsonDS1);
        SingleOutputStreamOperator<JSONObject> userInfoSupMsgOutputDS = assignTimestampsAndWatermarks(dbJsonDS2);


        SingleOutputStreamOperator<JSONObject> userInfoOutputDS1 = removeSourceFields(userInfoOutputDS);
        SingleOutputStreamOperator<JSONObject> userInfoSupMsgOutputDS1 = removeSourceFields(userInfoSupMsgOutputDS);

        //1747035718684
//        userInfoOutputDS1.print("userInfoOutputDS1"+userInfoOutputDS1);
//        userInfoSupMsgOutputDS1.print("userInfoSupMsgOutputDS1"+userInfoSupMsgOutputDS1);
        // 异步io
        SingleOutputStreamOperator<JSONObject> userInfo = userInfoOutputDS1
                .keyBy(o->o.getString("id"))
                .intervalJoin(userInfoSupMsgOutputDS1.keyBy(o->o.getString("uid")))
                .between(Time.minutes(-30), Time.minutes(30))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        // 对数据进行转换，如用户的生日进行转换
                        if (jsonObject1 != null && jsonObject1.containsKey("birthday")) {
                            Integer epochDay = jsonObject1.getInteger("birthday");
                            if (epochDay != null) {
                                LocalDate date = LocalDate.ofEpochDay(epochDay);
                                jsonObject1.put("birthday", date.format(DateTimeFormatter.ISO_DATE));

                            }
                        }
                        if (jsonObject1 != null) {
                            jsonObject1.put("user_info_sup_msg", jsonObject2);
                            String birthdayStr = jsonObject1.getString("birthday");
                            String substring = birthdayStr.substring(0,3);

                            // 获取年份
                            jsonObject1.put("nianDai", substring + "0");

                            // 获取年龄
                            LocalDate birthday = LocalDate.parse(birthdayStr, DateTimeFormatter.ISO_DATE);
                            LocalDate currentDate = LocalDate.now(ZoneId.of("Asia/Shanghai"));
                            int age = calculateAge(birthday, currentDate);
                            jsonObject1.put("age",age);

                            // 获取性别
                            if(jsonObject1.getString("gender")==null){
                                jsonObject1.put("gender","H");
                            }
                        }
                        collector.collect(jsonObject1);

                    }
                });
        userInfo.print();

        env.execute();
    }
    public static SingleOutputStreamOperator<JSONObject> removeSourceFields(SingleOutputStreamOperator<JSONObject> userInfoOutputDS1) {
        return userInfoOutputDS1.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject after = jsonObject.getJSONObject("after");
                after.put("op",jsonObject.getString("op"));
                after.put("ts",jsonObject.getString("ts_ms"));
                return after;
            }
        });
    }
    private static int calculateAge(LocalDate birthDate, LocalDate currentDate) {
        return Period.between(birthDate, currentDate).getYears();
    }
    public static SingleOutputStreamOperator<JSONObject> assignTimestampsAndWatermarks(SingleOutputStreamOperator<JSONObject> userInfoOutputDS) {
        return userInfoOutputDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts_ms");
                            }
                        })
        );
    }
}
