package com.sdy.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sdy.domain.Constant;
import com.sdy.domin.DimBaseCategory;
import com.sdy.domin.DimBaseCategory2;
import com.sdy.func.*;
import com.sdy.utils.*;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;


/**
 * @Package com.sdy.dwd.label2kafka
 * @Author danyu-shi
 * @Date 2025/5/12 9:52
 * @description:
 */
public class DbusUserInfo6BaseLabel {

    private static final List<DimBaseCategory> dim_base_categories;

    private static final List<DimBaseCategory2> dim_base_categories2;

    private static final Connection connection;

    private static final double device_rate_weight_coefficient = 0.1; // 设备权重系数
    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数

    //从三张分类表（base_category3、base_category2、base_category1）中联合查询分类名称
    static {
        try {
            connection = JdbcUtils.getMySQLConnection(
                    Constant.MYSQL_URL,
                    Constant.MYSQL_USER_NAME,
                    Constant.MYSQL_PASSWORD
            );
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

            String sql2 = "select\n" +
                    "    oi.id as order_id,\n" +
                    "    oi.user_id as uid,\n" +
                    "    kpd.base_category_name as  bcname,\n" +
                    "    kpd.base_trademark_name as btname,\n" +
                    "    od.order_price as price ,\n" +
                    "    oi.create_time as create_time\n" +
                    "\n" +
                    "from gmall_v1_danyu_shi.order_info oi\n" +
                    "left join gmall_v1_danyu_shi.order_detail od\n" +
                    "on oi.id=od.order_id\n" +
                    "left join gmall_v1_danyu_shi.sku_info ki\n" +
                    "on od.sku_id=ki.id\n" +
                    "left join gmall_v1_danyu_shi.hbase_kpb kpd\n" +
                    "on ki.category3_id=kpd.base_category_id;";
            dim_base_categories2 = JdbcUtils.queryList2(connection, sql2, DimBaseCategory2.class, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


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

//        keyedStreamLogPageMsg.print();

        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsData());

        // 2 min 分钟窗口
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(data -> data.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");
//        PageLog----->> {"uid":"115","os":"iOS,Android","ch":"Appstore,web,wandoujia,vivo","pv":12,"md":"iPhone 14,vivo IQOO Z6x ,vivo x90,SAMSUNG Galaxy s22","search_item":"","ba":"iPhone,vivo,SAMSUNG"}
//        win2MinutesPageLogsDs.print("PageLog----->");
        // 设备打分模型
        // {"device_35_39":0.04,"os":"iOS,Android","device_50":0.02,"search_25_29":0,"ch":"Appstore,xiaomi","pv":13,"device_30_34":0.05,"device_18_24":0.07,"search_50":0,"search_40_49":0,"uid":"20","device_25_29":0.06,"md":"iPhone 14,vivo x90,xiaomi 12 ultra ","search_18_24":0,"judge_os":"iOS","search_35_39":0,"device_40_49":0.03,"search_item":"","ba":"iPhone,xiaomi,vivo","search_30_34":0}
        SingleOutputStreamOperator<JSONObject> MinutesPageLogsDS = win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));
//        MinutesPageLogsDS.print();

        //价格区间

        // 订单数据
        SingleOutputStreamOperator<JSONObject> orderInfoDs = dataConvertJsonDs
                .filter(json -> json.getJSONObject("source").getString("table").equals("order_info"));

        SingleOutputStreamOperator<JSONObject> orderInfoUpdDs = orderInfoDs.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                String op = jsonObject.getString("op");
                JSONObject json = new JSONObject();
                if (!op.equals("d")) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    json.put("op", op);
                    json.put("order_id", after.getString("id"));
                    json.put("create_time", after.getString("create_time"));
                    json.put("total_amount", after.getString("total_amount"));
                    json.put("uid", after.getString("user_id"));
                    json.put("ts_ms", jsonObject.getString("ts_ms"));
                    return json;

                }
                return null;
            }
        });

        SingleOutputStreamOperator<JSONObject> orderDetailDs = dataConvertJsonDs
                .filter(json -> json.getJSONObject("source").getString("table").equals("order_detail"));

        SingleOutputStreamOperator<JSONObject> orderDetailUpdDs = orderDetailDs.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                String op = jsonObject.getString("op");
                JSONObject json = new JSONObject();
                if (!op.equals("d")) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    json.put("op", op);
                    json.put("order_id", after.getString("order_id"));
                    json.put("ts_ms", jsonObject.getString("ts_ms"));
                    json.put("sku_id", after.getString("sku_id"));
                    return json;

                }
                return null;
            }
        });

        SingleOutputStreamOperator<JSONObject> orderDetail2 = orderInfoUpdDs
                .keyBy(o->o.getString("order_id"))
                .intervalJoin(orderDetailUpdDs.keyBy(o->o.getString("order_id")))
                .between(Time.days(-30), Time.days(30))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        jsonObject1.put("sku_id",jsonObject2.getString("sku_id"));

                        collector.collect(jsonObject1);
                    }
                });

        //通过布隆过滤器，去掉重复数据
        SingleOutputStreamOperator<JSONObject> orderDetail1 = orderDetail2.keyBy(data -> data.getLong("order_id"))
                .filter(new FilterBloomOrderInfolicatorFunc(1000000, 0.01));
//        orderDetail1.print();
//        根据订单详情计算用户在不同价格区间的消费打分；利用 dim_base_categories2 进行商品类目匹配；结合设备和搜索行为的权重系数，生成用户的标签化订单特征信息
        SingleOutputStreamOperator<JSONObject> orderInfos = orderDetail1.map(new MaoOrderInfo(dim_base_categories2, device_rate_weight_coefficient, search_rate_weight_coefficient));
//        orderInfos.print();


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
//--------------------------------------------------------------------->
//
//        通过用户的 uid，将 processIntervalJoinUserInfo6BaseMessageDs 和 orderInfos 两个数据流进行时间区间内的关联（Interval Join），关联的时间范围是前后各30小时。
//        在关联过程中，将 orderInfos 流中的订单相关的标签（如 sum_25~29、sum_40~49 等）添加到用户信息对象中，然后输出增强后的用户信息。

        SingleOutputStreamOperator<JSONObject> user = processIntervalJoinUserInfo6BaseMessageDs
                .keyBy(o->o.getString("uid"))
                .intervalJoin(orderInfos.keyBy(o->o.getString("uid")))
                .between(Time.hours(-30), Time.hours(30))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        jsonObject1.put("sum_25~29",jsonObject2.getString("sum_25~29"));
                        jsonObject1.put("sum_40~49",jsonObject2.getString("sum_40~49"));
                        jsonObject1.put("sum_50",jsonObject2.getString("sum_50"));
                        jsonObject1.put("sum_18~24",jsonObject2.getString("sum_18~24"));
                        jsonObject1.put("sum_35~39",jsonObject2.getString("sum_35~39"));
                        jsonObject1.put("sum_30~34",jsonObject2.getString("sum_30~34"));

                        collector.collect(jsonObject1);
                    }
                });

//        user.print();



//        设置时间戳：从MinutesPageLogsDS流中提取每条数据的uid和ts字段，将ts赋值给ts_ms作为事件时间戳；
//        分配水位线：使用forMonotonousTimestamps策略生成水位线，基于ts_ms字段进行时间窗口操作，用于后续基于事件时间的窗口处理。
        SingleOutputStreamOperator<JSONObject> logs1 = MinutesPageLogsDS.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                String uid = jsonObject.getString("uid");
//                jsonObject.put("uid", uid.substring(0, 1));
                jsonObject.put("ts_ms", jsonObject.getString("ts"));
                return jsonObject;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLongValue("ts_ms");
            }
        }));

//        logs1.print();
//        通过 uid 将两个数据流 user 和 logs1 在 [-12小时, 12小时] 的时间窗口内进行 Interval Join，
//        然后在 processElement 方法中将日志数据（jsonObject2）中的搜索和设备标签加到用户数据（jsonObject1）的对应字段中，最终输出增强后的用户信息。

        SingleOutputStreamOperator<JSONObject> user2 = user
                .keyBy(o->o.getString("uid"))
                .intervalJoin(logs1.keyBy(o->o.getString("uid")))
                .between(Time.hours(-12), Time.hours(12))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        jsonObject1.put("sum_25~29",jsonObject1.getDoubleValue("sum_25~29")+ jsonObject2.getDoubleValue("search_18_24") + jsonObject2.getDoubleValue("device_18_24"));
                        jsonObject1.put("sum_40~49",jsonObject1.getDoubleValue("sum_40~49")+ jsonObject2.getDoubleValue("search_40_49") + jsonObject2.getDoubleValue("device_40_49"));
                        jsonObject1.put("sum_50",jsonObject1.getDoubleValue("sum_50") + jsonObject2.getDoubleValue("search_50") + jsonObject2.getDoubleValue("device_50"));
                        jsonObject1.put("sum_18~24",jsonObject1.getDoubleValue("sum_18~24") + jsonObject2.getDoubleValue("device_18_24") + + jsonObject2.getDoubleValue("search_18_24"));
                        jsonObject1.put("sum_35~39",jsonObject1.getDoubleValue("sum_35~39") + jsonObject2.getDoubleValue("search_35_39") + jsonObject2.getDoubleValue("device_35_39"));
                        jsonObject1.put("sum_30~34",jsonObject1.getDoubleValue("sum_30~34") + jsonObject2.getDoubleValue("search_30_34") + jsonObject2.getDoubleValue("device_30_34"));
                        collector.collect(jsonObject1);
                    }
                });
//        user2.print("zzzzz->>>");


//        该段代码实现了基于用户ID（uid）的数据去重处理，保留每个用户最新的记录
        SingleOutputStreamOperator<JSONObject> dedupedUser2 = user2
                .keyBy(o -> o.getString("uid"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private transient ValueState<Long> lastProcessedTs;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastProcessedTs = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("lastProcessedTs", Long.class));
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        long currentTs = value.getLongValue("final_process_ts");
                        Long lastTs = lastProcessedTs.value();

                        // 只输出时间戳更新的数据（或首次出现的数据）
                        if (lastTs == null || currentTs > lastTs) {
                            lastProcessedTs.update(currentTs);
                            out.collect(value);
                        }
                    }
                });

        // 输出结果
        dedupedUser2.print("final_result->>>");




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