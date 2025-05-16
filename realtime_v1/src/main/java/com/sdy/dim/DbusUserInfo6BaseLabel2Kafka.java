package com.sdy.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sdy.domain.Constant;
import com.sdy.domin.DimBaseCategory;
import com.sdy.func.*;
import com.sdy.utils.*;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;


/**
 * @Package com.label.DbusUserInfo6BaseLabel
 * @Author danyu.shi
 * @Date 2025/5/12 10:01
 * @description: 01 Task 6 BaseLine
 */

public class DbusUserInfo6BaseLabel2Kafka {

    private static final List<DimBaseCategory> dim_base_categories;
    private static final Connection connection;

    private static final double device_rate_weight_coefficient = 0.1; // 设备权重系数
    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数
    private static final double time_rate_weight_coefficient = 0.1;    // 时间权重系数
    private static final double amount_rate_weight_coefficient = 0.15;    // 价格权重系数
    private static final double brand_rate_weight_coefficient = 0.2;    // 品牌权重系数
    private static final double category_rate_weight_coefficient = 0.3; // 类目权重系数

    //从三张分类表（base_category3、base_category2、base_category1）中联合查询分类名称

    static {
        try {
            connection = JdbcUtils.getMySQLConnection(
                    Constant.MYSQL_URL,
                    Constant.MYSQL_USER_NAME,
                    Constant.MYSQL_PASSWORD);
            String sql = "select b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     from realtime_v1.base_category3 as b3  \n" +
                    "     join realtime_v1.base_category2 as b2  \n" +
                    "     on b3.category2_id = b2.id             \n" +
                    "     join realtime_v1.base_category1 as b1  \n" +
                    "     on b2.category1_id = b1.id";
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @SneakyThrows
    public static void main(String[] args) {
//设置Hadoop的用户名称为"root"，用于在Hadoop集群中进行身份识别和权限控制
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // user info cdc
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
        );

        // page log
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
        );

        //Kafka 消息流（字符串形式）；
        //使用 map(JSON::parseObject) 将每条消息解析为 JSONObject
        SingleOutputStreamOperator<JSONObject> dataConvertJsonDs = kafkaCdcDbSource.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> dataPageLogConvertJsonDs = kafkaPageLogSource.map(JSON::parseObject);

        // 设备信息 + 关键词搜索
        SingleOutputStreamOperator<JSONObject> logDeviceInfoDs = dataPageLogConvertJsonDs.map(new MapDeviceInfoAndSearchKetWordMsg())
;

//过滤出包含非空uid字段的数据；按UID分组
        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = logDeviceInfoDs.filter(data -> !data.getString("uid").isEmpty());
        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(data -> data.getString("uid"));

//过滤掉相同 UID 下重复的 ts_ms 时间戳数据；避免同一用户短时间内重复行为数据的影响（去重逻辑基于时间戳）
        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsData());

        // 2 min 分钟窗口
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(data -> data.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                //滚动窗口对每个用户的日志数据进行聚合处理
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2);
//        222----->> {"uid":"114","os":"Android","ch":"xiaomi,oppo,web","pv":22,"md":"xiaomi 13,vivo IQOO Z6x ,xiaomi 13 Pro ,Redmi k50","search_item":"","ba":"xiaomi,vivo,Redmi","ts":"1747214717593"}
//        win2MinutesPageLogsDs.print("222----->");
        // 设备打分模型 base2
        // 数据流应用映射函数，传入了品类维度数据和两个权重系数。
        SingleOutputStreamOperator<JSONObject> mapDeviceAndSearchRateResultDs = win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));
//        mapDeviceAndSearchRateResultDs.print("设备信息 + 关键词搜索---->");



//"user_info"
        SingleOutputStreamOperator<JSONObject> userInfoDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("user_info"));
//        userInfoDs.print();
        SingleOutputStreamOperator<JSONObject> cdcOrderInfoDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("order_info"));
//        cdcOrderInfoDs.print();
        SingleOutputStreamOperator<JSONObject> cdcOrderDetailDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("order_detail"));
//        cdcOrderDetailDs.print();
        SingleOutputStreamOperator<JSONObject> mapCdcOrderInfoDs = cdcOrderInfoDs.map(new MapOrderInfoDataFunc());
//        mapCdcOrderInfoDs.print();
        SingleOutputStreamOperator<JSONObject> mapCdcOrderDetailDs = cdcOrderDetailDs.map(new MapOrderDetailFunc());
//        mapCdcOrderDetailDs.print();
        SingleOutputStreamOperator<JSONObject> filterNotNullCdcOrderInfoDs = mapCdcOrderInfoDs.filter(data -> data.getString("id") != null && !data.getString("id").isEmpty());
//        filterNotNullCdcOrderInfoDs.print();
        SingleOutputStreamOperator<JSONObject> filterNotNullCdcOrderDetailDs = mapCdcOrderDetailDs.filter(data -> data.getString("order_id") != null && !data.getString("order_id").isEmpty());
//        filterNotNullCdcOrderDetailDs.print();
        KeyedStream<JSONObject, String> keyedStreamCdcOrderInfoDs = filterNotNullCdcOrderInfoDs.keyBy(data -> data.getString("id"));
//        keyedStreamCdcOrderInfoDs.print();

        KeyedStream<JSONObject, String> keyedStreamCdcOrderDetailDs = filterNotNullCdcOrderDetailDs.keyBy(data -> data.getString("order_id"));
//        keyedStreamCdcOrderDetailDs.print();
        SingleOutputStreamOperator<JSONObject> processIntervalJoinOrderInfoAndDetailDs = keyedStreamCdcOrderInfoDs.intervalJoin(keyedStreamCdcOrderDetailDs)
                .between(Time.hours(-2), Time.hours(2))
                .process(new IntervalDbOrderInfoJoinOrderDetailProcessFunc());

        SingleOutputStreamOperator<JSONObject> processDuplicateOrderInfoAndDetailDs = processIntervalJoinOrderInfoAndDetailDs.keyBy(data -> data.getString("detail_id"))
                .process(new processOrderInfoAndDetailFunc());
        // 品类 品牌 年龄 时间 base4
        SingleOutputStreamOperator<JSONObject> mapOrderInfoAndDetailModelDs = processDuplicateOrderInfoAndDetailDs.map(new MapOrderAndDetailRateModelFunc(dim_base_categories, time_rate_weight_coefficient, amount_rate_weight_coefficient, brand_rate_weight_coefficient, category_rate_weight_coefficient));
//        4---->> {"b1name_25-29":0.24,"sku_num":1,"pay_time_30-34":0.01,"tname_30-34":0.14,"amount_18-24":0.015,"original_total_amount":"21695.0","amount_30-34":0.045,"b1name_50":0.03,"pay_time_50":0.03,"uid":"1070","pay_time_25-29":0.01,"tname_18-24":0.18,"tname_40-49":0.14,"b1_name":"手机","pay_time_18-24":0.01,"b1name_30-34":0.18,"b1name_40-49":0.06,"tname_25-29":0.16,"sku_name":"Apple iPhone 12 (A2404) 64GB 蓝色 支持移动联通电信5G 双卡双待手机","pay_time_40-49":0.02,"id":"3708","b1name_18-24":0.27,"consignee":"周艳瑞","pay_time_35-39":0.01,"create_time":1745447489000,"c3id":"61","tname_35-39":0.14,"split_coupon_amount":"1","tname":"苹果","order_price":"8197.0","sku_id":10,"detail_id":"5305","amount_35-39":0.06,"amount_50":0.09,"tname_50":0.1,"b1name_35-39":0.12,"pay_time_slot":"早晨","amount_25-29":0.03,"total_amount":"21195.0","province_id":28,"order_id":"3708","amount_40-49":0.075,"ts_ms":1747019726073,"split_activity_amount":0.0,"split_total_amount":8197.0}
//        mapOrderInfoAndDetailModelDs.print("4---->");


//        生日字段转换为标准日期格
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

        SingleOutputStreamOperator<JSONObject> userInfoSupDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("user_info_sup_msg"));
        //生日，计算用户的年龄（age）和年代（decade）。
        //根据出生日期确定星座
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
                });
//        mapUserInfoDs.print();
        //数据中提取 after 字段中的用户补充信息（如身高、体重等
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
                });


        //数据流进行过滤，保留包含非空uid字段的元素
        SingleOutputStreamOperator<JSONObject> finalUserinfoDs = mapUserInfoDs.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());
        SingleOutputStreamOperator<JSONObject> finalUserinfoSupDs = mapUserInfoSupDs.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());
//将用户基本信息流按 uid 分组
        KeyedStream<JSONObject, String> keyedStreamUserInfoDs = finalUserinfoDs.keyBy(data -> data.getString("uid"));
       //将用户补充信息流按 uid 分组
        KeyedStream<JSONObject, String> keyedStreamUserInfoSupDs = finalUserinfoSupDs.keyBy(data -> data.getString("uid"));
//        keyedStreamUserInfoDs.print("u----->");
//        keyedStreamUserInfoSupDs.print("sup------>");
        // base6Line

        /*
        {"birthday":"1979-07-06","decade":1970,"uname":"鲁瑞","gender":"home","zodiac_sign":"巨蟹座","weight":"52","uid":"302","login_name":"9pzhfy3admw3","unit_height":"cm","user_level":"1","phone_num":"13275315996","unit_weight":"kg","email":"9pzhfy3admw3@gmail.com","ts_ms":1747052360573,"age":45,"height":"164"}
        {"birthday":"2005-08-12","decade":2000,"uname":"潘国","gender":"M","zodiac_sign":"狮子座","weight":"68","uid":"522","login_name":"toim614z6zf","unit_height":"cm","user_level":"1","phone_num":"13648187991","unit_weight":"kg","email":"toim614z6zf@hotmail.com","ts_ms":1747052368281,"age":19,"height":"181"}
        {"birthday":"1997-09-06","decade":1990,"uname":"南宫纨","gender":"F","zodiac_sign":"处女座","weight":"53","uid":"167","login_name":"4tjk9p8","unit_height":"cm","user_level":"1","phone_num":"13913669538","unit_weight":"kg","email":"hij36hcc@3721.net","ts_ms":1747052360467,"age":27,"height":"167"}
        */
        //年龄、性别、年代、身高、体重、星座 6 类标签

        SingleOutputStreamOperator<JSONObject> processIntervalJoinUserInfo6BaseMessageDs = keyedStreamUserInfoDs.intervalJoin(keyedStreamUserInfoSupDs)
                .between(Time.days(-5), Time.days(5))
                .process(new IntervalJoinUserInfoLabelProcessFunc());

        processIntervalJoinUserInfo6BaseMessageDs.print();


        // 不分层代码实现 数据量计算太大，查表数据和kafka主题太多，优化数据链路，使用kafka作为中间件进行数据处理
        /*
        SingleOutputStreamOperator<JSONObject> filterOrderInfoAndDetail4BaseModelDs = mapOrderInfoAndDetailModelDs.filter(data -> data.getString("uid") != null && data.isEmpty());
        SingleOutputStreamOperator<JSONObject> filterDevice2BaseModelDs = mapDeviceAndSearchRateResultDs.filter(data -> data.getString("uid") != null && data.isEmpty());

        KeyedStream<JSONObject, String> keyedStreamOrderInfoAndDetail4BaseModelDs = filterOrderInfoAndDetail4BaseModelDs.keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedStreamDevice2BaseModelDs = filterDevice2BaseModelDs.keyBy(data -> data.getString("uid"));*/

        /*
        keyedStreamOrderInfoAndDetail4BaseModelDs.intervalJoin(keyedStreamDevice2BaseModelDs)
                        .between(Time.minutes(-2), Time.minutes(2))
                                .process(new Interval4BaseJoin2BaseFunc()).print();
                                */



//        processIntervalJoinUserInfo6BaseMessageDs.map(data -> data.toJSONString())
//                        .sinkTo(
//                                KafkaUtils.buildKafkaSink("kafka_botstrap_servers","kafka_label_base6_topic")
//                        );
//
//        mapOrderInfoAndDetailModelDs.map(data -> data.toJSONString())
//                        .sinkTo(
//                                KafkaUtils.buildKafkaSink("kafka_botstrap_servers","kafka_label_base4_topic")
//                        );
//
//        mapDeviceAndSearchRateResultDs.map(data -> data.toJSONString())
//                        .sinkTo(
//                                KafkaUtils.buildKafkaSink("kafka_botstrap_servers","kafka_label_base2_topic")
//                        );



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