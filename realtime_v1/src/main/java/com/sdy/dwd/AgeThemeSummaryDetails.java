package com.sdy.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sdy.domain.Constant;
import com.sdy.utils.FlinkSourceUtil;
import com.sdy.utils.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.sdy.dwd.AgeThemeSummaryDetails
 * @Author danyu-shi
 * @Date 2025/5/12 19:51
 * @description:
 */
public class AgeThemeSummaryDetails {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2.检查点相关的设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));

        DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "realtime-gd-danyu", "my-group");

        SingleOutputStreamOperator<JSONObject> user_info_fifter = kafkaSource.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("user_info"));
        //处理用户表生日字段
        SingleOutputStreamOperator<JSONObject> result_user = user_info_fifter.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                // 从输入的 JSON 对象中获取 "after" 字段，该字段通常包含数据库变更后的数据
                JSONObject afterObj = jsonObject.getJSONObject("after");

                // 如果 afterObj 不为空且包含 birthday 字段，则进行处理
                if (afterObj != null && afterObj.containsKey("birthday")) {
                    // 获取 birthday 字段的值，可能是字符串或数字形式的出生日期
                    Object birthdayValue = afterObj.get("birthday");

                    // 如果 birthdayValue 不为 null，则尝试将其转换为标准日期格式
                    if (birthdayValue != null) {
                        // 将 birthday 转换为字符串形式以便后续解析
                        String daysSinceBaseStr = birthdayValue.toString();

                        try {
                            // 尝试将字符串解析为表示自 1970-01-01 起的天数的 long 值
                            long days = Long.parseLong(daysSinceBaseStr);

                            // 定义基准日期（Unix 时间起点）
                            LocalDate baseDate = LocalDate.of(1970, 1, 1);

                            // 计算实际出生日期
                            LocalDate birthDate = baseDate.plusDays(days);

                            // 使用 ISO 标准格式化日期（如：2023-04-05）
                            String formattedDate = birthDate.format(DateTimeFormatter.ISO_LOCAL_DATE);

                            // 将原始的 birthday 字段替换为格式化后的日期字符串
                            afterObj.put("birthday", formattedDate);
                        } catch (NumberFormatException e) {
                            // 如果解析失败（例如字段不是有效的数字），则标记生日为无效
                            afterObj.put("birthday", "invalid_date");
                        }
                    }
                }

                // 返回处理后的 JSON 对象
                return jsonObject;
            }
        });
//        result_user.print("处理完用户表生日字段 ");
//        4> {"op":"c","after":{"birthday":"1996-10-12","gender":"F","create_time":1747087262000,"login_name":"xdxvmb3kf","nick_name":"倩婷","name":"曹倩婷","user_level":"2","phone_num":"13838356998","id":89,"email":"xdxvmb3kf@googlemail.com"},"source":{"thread":1301,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000046","connector":"mysql","pos":63126213,"name":"mysql_binlog_source","row":0,"ts_ms":1747030156000,"snapshot":"false","db":"realtime_v2","table":"user_info"},"ts_ms":1747030155352}
        //过滤 一级  类目
        SingleOutputStreamOperator<JSONObject> base_category1 = kafkaSource.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("base_category1"));
//        base_category1.print();

        //过滤二级 类目
        SingleOutputStreamOperator<JSONObject> base_category2 = kafkaSource.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("base_category2"));
//        base_category2.print();

        //过滤三级 类目

        SingleOutputStreamOperator<JSONObject> base_category3 = kafkaSource.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("base_category3"));

//        base_category3.print();


        // 过滤 品牌 表
        SingleOutputStreamOperator<JSONObject> base_trademark = kafkaSource.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("base_trademark"));

//        base_trademark.print();


        // 过滤 明细表价格 order_detail
        SingleOutputStreamOperator<JSONObject> order_detail = kafkaSource.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("order_detail"));
//        order_detail.print();


        //过滤订单表
        SingleOutputStreamOperator<JSONObject> order_info = kafkaSource.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> jsonnObj.getJSONObject("source").getString("table").equals("order_info"));
//        order_info.print();


            KafkaSource<String> kafkaSource01 = FlinkSourceUtil.getKafkaSource("stream-dev1-danyushi", "gmall0");
        DataStreamSource<String> kafkaDs = env.fromSource(kafkaSource01, WatermarkStrategy.noWatermarks(), "kafkaSource");
        kafkaDs.print();


//        4> {"op":"c","after":{"category1_id":17,"create_time":1639440000000,"name":"健身训练","id":112},"source":{"thread":1254,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000046","connector":"mysql","pos":60260558,"name":"mysql_binlog_source","row":0,"ts_ms":1747030130000,"snapshot":"false","db":"realtime_v2","table":"base_category2"},"ts_ms":1747030128936}
        //过滤搜索词 和设备信息
        SingleOutputStreamOperator<JSONObject> word = kafkaDs.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonnObj -> {
            // 安全获取 page 对象
            JSONObject pageObj = jsonnObj.getJSONObject("page");
            JSONObject common = jsonnObj.getJSONObject("common");

            if (pageObj == null &&common == null) {
                return false; // 如果 page 不存在，过滤掉这条数据
            }

            // 安全获取字段值并进行判断
            String itemType = pageObj.getString("item_type");
            String lastPageId = pageObj.getString("last_page_id");
            String md = common.getString("md");

            // 使用 Objects.equals 防止 Null
            return "keyword".equals(itemType) && "search".equals(lastPageId) && md != null;
        });
//        word.print();


        env.execute();


    }
}
