package com.sdy.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sdy.domain.Constant;
import com.sdy.func.AggregateUserDataProcessFunction;
import com.sdy.func.MapDeviceInfoAndSearchKetWordMsg;
import com.sdy.func.ProcessFilterRepeatTsData;
import com.sdy.utils.KafkaUtil;
import com.sdy.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import com.sdy.func.IntervalJoinUserInfoLabelProcessFunc;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;


/**
 * @Package com.sdy.dwd.label2kafka
 * @Author danyu-shi
 * @Date 2025/5/12 9:52
 * @description:
 */
public class DbusUserInfo6BaseLabel {

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        Constant.KAFKA_BROKERS,
                        Constant.TOPIC_DB,
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
                "kafka_cdc_db_source"
        ).uid("kafka_cdc_db_source").name("kafka_cdc_db_source");

//        kafka--->:12> {"before":null,"after":{"id":7,"tm_name":"金沙河","logo_url":"/static/default.jpg","create_time":1639440000000,"operate_time":null},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall_v1_danyu_shi","sequence":null,"table":"base_trademark","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1747051824730,"transaction":null}
//        kafkaCdcDbSource.print("kafka--->");


        SingleOutputStreamOperator<String> kafkaPageLogSource = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        Constant.KAFKA_BROKERS,
                        Constant.TOPIC_LOG,
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
                        "kafka_page_log_source"
                ).uid("kafka_page_log_source")
                .name("kafka_page_log_source");
//        pageLog--->:7> {"common":{"ar":"29","ba":"iPhone","ch":"Appstore","is_new":"1","md":"iPhone 14","mid":"mid_31","os":"iOS 13.3.1","sid":"97e2d4e1-1dd1-48d3-adb1-2908ef91d3ce","uid":"819","vc":"v2.1.134"},"page":{"during_time":15000,"item":"819","item_type":"user_id","last_page_id":"good_detail","page_id":"register"},"ts":1745162898593}
//        kafkaPageLogSource.print("pageLog--->");

        SingleOutputStreamOperator<JSONObject> dataConvertJsonDs = kafkaCdcDbSource.map(JSON::parseObject)
                .uid("convert json cdc db")
                .name("convert json cdc db");

        SingleOutputStreamOperator<JSONObject> dataPageLogConvertJsonDs = kafkaPageLogSource.map(JSON::parseObject)
                .uid("convert json page log")
                .name("convert json page log");

        // 设备信息 + 关键词搜索
        SingleOutputStreamOperator<JSONObject> logDeviceInfoDs = dataPageLogConvertJsonDs.map(new MapDeviceInfoAndSearchKetWordMsg())
                .uid("get device info & search")
                .name("get device info & search");
//        log---->:7> {"uid":"777","deviceInfo":{"ar":"22","uid":"777","os":"Android","ch":"web","md":"xiaomi 12 ultra ","vc":"v2.1.134","ba":"xiaomi"},"ts":1745162151520}
//        logDeviceInfoDs.print("log---->");

        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = logDeviceInfoDs.filter(data -> !data.getString("uid").isEmpty());
        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(data -> data.getString("uid"));


        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsData());

        // 2 min 分钟窗口
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(data -> data.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");

//        win2MinutesPageLogsDs.print("PageLog----->");




        SingleOutputStreamOperator<JSONObject> userInfoDs = dataConvertJsonDs.
                filter(data -> data.getJSONObject("source").getString("table").equals("user_info"))
                .uid("filter kafka user info")
                .name("filter kafka user info");

//        user_info--->:10> {"op":"r","after":{"birthday":265,"gender":"M","create_time":1745439012000,"login_name":"zwxbc7nkv","nick_name":"阿磊","name":"范磊","user_level":"1","phone_num":"13654267715","id":1047,"email":"zwxbc7nkv@gmail.com"},"source":{"server_id":0,"version":"1.6.4.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall_v1_danyu_shi","table":"user_info"},"ts_ms":1747035718684}
//        userInfoDs.print("user_info--->");

        SingleOutputStreamOperator<JSONObject> finalUserInfoDs = userInfoDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject){
                JSONObject after = jsonObject.getJSONObject("after");
                if (after != null && after.containsKey("birthday")) {
                    Integer epochDay = after.getInteger("birthday");
                    if (epochDay != null) {
                        LocalDate date = LocalDate.ofEpochDay(epochDay);
                        after.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                    }
                }
                return jsonObject;
            }
        });
//        10> {"op":"r","after":{"birthday":"1997-08-23","gender":"M","create_time":1745433264000,"login_name":"b4uxl29g","nick_name":"泰盛","name":"苗泰盛","user_level":"1","phone_num":"13366757541","id":1044,"email":"b4uxl29g@yeah.net"},"source":{"server_id":0,"version":"1.6.4.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall_v1_danyu_shi","table":"user_info"},"ts_ms":1747035718684}
//        finalUserInfoDs.print();


        SingleOutputStreamOperator<JSONObject> userInfoSupDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("user_info_sup_msg"))
                .uid("filter kafka user info sup")
                .name("filter kafka user info sup");

        SingleOutputStreamOperator<JSONObject> mapUserInfoDs = finalUserInfoDs.map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject){
                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            result.put("uid", after.getString("id"));
                            result.put("uname", after.getString("name"));
                            result.put("user_level", after.getString("user_level"));
                            result.put("login_name", after.getString("login_name"));
                            result.put("phone_num", after.getString("phone_num"));
                            result.put("email", after.getString("email"));
                            result.put("gender", after.getString("gender") != null ? after.getString("gender") : "home");
                            result.put("birthday", after.getString("birthday"));
                            result.put("ts_ms", jsonObject.getLongValue("ts_ms"));
                            String birthdayStr = after.getString("birthday");
                            if (birthdayStr != null && !birthdayStr.isEmpty()) {
                                try {
                                    LocalDate birthday = LocalDate.parse(birthdayStr, DateTimeFormatter.ISO_DATE);
                                    LocalDate currentDate = LocalDate.now(ZoneId.of("Asia/Shanghai"));
                                    int age = calculateAge(birthday, currentDate);
                                    int decade = birthday.getYear() / 10 * 10;
                                    result.put("decade", decade);
                                    result.put("age", age);
                                    String zodiac = getZodiacSign(birthday);
                                    result.put("zodiac_sign", zodiac);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }

                        return result;
                    }
                }).uid("map userInfo ds")
                .name("map userInfo ds");

//        UserInfo---->:10> {"birthday":"2000-10-23","uid":"1046","decade":2000,"login_name":"xa7dvia80","uname":"常新利","gender":"home","zodiac_sign":"天秤座","user_level":"1","phone_num":"13742132159","email":"xa7dvia80@googlemail.com","ts_ms":1747035718684,"age":24}
//        mapUserInfoDs.print("UserInfo---->");

        SingleOutputStreamOperator<JSONObject> mapUserInfoSupDs = userInfoSupDs.map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) {
                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            result.put("uid", after.getString("uid"));
                            result.put("unit_height", after.getString("unit_height"));
                            result.put("create_ts", after.getLong("create_ts"));
                            result.put("weight", after.getString("weight"));
                            result.put("unit_weight", after.getString("unit_weight"));
                            result.put("height", after.getString("height"));
                            result.put("ts_ms", jsonObject.getLong("ts_ms"));
                        }
                        return result;
                    }
                }).uid("sup userinfo sup")
                .name("sup userinfo sup");
//        spu->>>:11> {"uid":"1045","unit_height":"cm","create_ts":1747043816000,"weight":"63","unit_weight":"kg","ts_ms":1747016080662,"height":"158"}
//        mapUserInfoSupDs.print("spu->>>");


        SingleOutputStreamOperator<JSONObject> finalUserinfoDs = mapUserInfoDs.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());
        SingleOutputStreamOperator<JSONObject> finalUserinfoSupDs = mapUserInfoSupDs.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());
//        Userinfo--->:10> {"birthday":"1976-06-23","uid":"1045","decade":1970,"login_name":"gsxz96s63s","uname":"俞翔","gender":"M","zodiac_sign":"巨蟹座","user_level":"1","phone_num":"13532331517","email":"gsxz96s63s@msn.com","ts_ms":1747035718684,"age":48}
//        finalUserinfoDs.print("Userinfo--->");
//        spu--->:11> {"uid":"1045","unit_height":"cm","create_ts":1747043816000,"weight":"63","unit_weight":"kg","ts_ms":1747016080662,"height":"158"}
//        finalUserinfoSupDs.print("spu--->");


        KeyedStream<JSONObject, String> keyedStreamUserInfoDs = finalUserinfoDs.keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedStreamUserInfoSupDs = finalUserinfoSupDs.keyBy(data -> data.getString("uid"));
//        kUserInfo--->:16> {"birthday":"1982-09-23","uid":"1029","decade":1980,"login_name":"h4vkj83","uname":"元伟刚","gender":"home","zodiac_sign":"天秤座","user_level":"1","phone_num":"13752945975","email":"h4vkj83@3721.net","ts_ms":1747035718684,"age":42}
//        keyedStreamUserInfoDs.print("kUserInfo--->");
//        kspu---->:3> {"uid":"901","unit_height":"cm","create_ts":1747043816000,"weight":"66","unit_weight":"kg","ts_ms":1747016080658,"height":"184"}
//        keyedStreamUserInfoSupDs.print("kspu---->");

//年龄、性别、年代、身高、体重、星座 6 类标签
// 17> {"birthday":"1972-01-23","decade":1970,"uname":"熊致树","gender":"home","zodiac_sign":"水瓶座","weight":"85","uid":"1028","login_name":"mnal6je","unit_height":"cm","user_level":"3","phone_num":"13515989633","unit_weight":"kg","email":"mnal6je@yahoo.com","ts_ms":1747035718684,"age":53,"height":"188"}

        SingleOutputStreamOperator<JSONObject> processIntervalJoinUserInfo6BaseMessageDs =
                keyedStreamUserInfoDs.intervalJoin(keyedStreamUserInfoSupDs)
                .between(Time.days(-1), Time.days(1))
                .process(new IntervalJoinUserInfoLabelProcessFunc());

//        processIntervalJoinUserInfo6BaseMessageDs.print();


        env.execute("DbusUserInfo6BaseLabel");
    }


    private static int calculateAge(LocalDate birthDate, LocalDate currentDate) {
        return Period.between(birthDate, currentDate).getYears();
    }

    private static String getZodiacSign(LocalDate birthDate) {
        int month = birthDate.getMonthValue();
        int day = birthDate.getDayOfMonth();

        // 星座日期范围定义
        if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) return "摩羯座";
        else if (month == 1 || month == 2 && day <= 18) return "水瓶座";
        else if (month == 2 || month == 3 && day <= 20) return "双鱼座";
        else if (month == 3 || month == 4 && day <= 19) return "白羊座";
        else if (month == 4 || month == 5 && day <= 20) return "金牛座";
        else if (month == 5 || month == 6 && day <= 21) return "双子座";
        else if (month == 6 || month == 7 && day <= 22) return "巨蟹座";
        else if (month == 7 || month == 8 && day <= 22) return "狮子座";
        else if (month == 8 || month == 9 && day <= 22) return "处女座";
        else if (month == 9 || month == 10 && day <= 23) return "天秤座";
        else if (month == 10 || month == 11 && day <= 22) return "天蝎座";
        else return "射手座";
    }
}