package com.sdy.dwd;

import com.sdy.common.utils.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.sdy.dwd.DwdCategorypreference
 * @Author danyu-shi
 * @Date 2025/5/12 10:27
 * @description: 类目偏好
 */
public class DwdCategorypreference {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "realtime-gd-danyu", "dwd_category_preference_group");
//        kafkaSource.print();
//        env.execute();


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "    `before` MAP<STRING, STRING>,\n" +
                "    `after` MAP<STRING, STRING>,\n" +
                "    `source` MAP<STRING, STRING>,\n" +
                "    `op` STRING,\n" +
                "    `ts_ms` STRING,\n" +
                "    proc_time AS proctime()\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'realtime-gd-danyu',\n" +
                "    'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json'\n" +
                ")");


//        tableEnv.executeSql("select * from topic_db").print();


        //order_info
        Table orderInfotable = tableEnv.sqlQuery("select " +
                " `after`['id'] id, " +
                " `after`['user_id'] user_id, " +
                " `op`," +
                " `ts_ms` " +
                "from topic_db " +
                "where `source`['table']='order_info'");
        tableEnv.createTemporaryView("order_info", orderInfotable);

//        orderInfotable.execute().print();


        //order_detail
        Table detailtable = tableEnv.sqlQuery("select " +
                " `after`['id'] id, " +
                " `after`['order_id'] order_id, " +
                " `after`['sku_id'] sku_id, " +
                " `op`," +
                " `ts_ms` " +
                "from topic_db " +
                "where `source`['table']='order_detail'");
        tableEnv.createTemporaryView("order_detail", detailtable);

//        detailtable.execute().print();


        Table usertable = tableEnv.sqlQuery("select " +
                " `after`['birthday'] birthday, " +
                " `after`['id'] id, " +
                " `after`['gender'] gender, " +
                " `after`['name'] name, " +
                " `op`," +
                " `ts_ms` " +
                "from topic_db " +
                "where `source`['table']='user_info'");
        tableEnv.createTemporaryView("user_info", usertable);
//        usertable.execute().print();


        Table skutable = tableEnv.sqlQuery("select " +
                " `after`['id'] id, " +
                " `after`['spu_id'] spu_id, " +
                " `after`['sku_name'] sku_name, " +
                " `after`['category3_id'] category3_id, " +
                " `op`," +
                " `ts_ms` " +
                "from topic_db " +
                "where `source`['table']='sku_info'");
        tableEnv.createTemporaryView("sku_info", skutable);
//        skutable.execute().print();



        Table c3table = tableEnv.sqlQuery("select " +
                " `after`['id'] id, " +
                " `after`['category2_id'] category2_id, " +
                " `after`['name'] c3_name, " +
                " `op`," +
                " `ts_ms` " +
                "from topic_db " +
                "where `source`['table']='base_category3'");
        tableEnv.createTemporaryView("base_category3 ", c3table);
//        c3table.execute().print();


        Table c2table = tableEnv.sqlQuery("select " +
                " `after`['id'] id, " +
                " `after`['category1_id'] category1_id, " +
                " `after`['name'] c2_name, " +
                " `op`," +
                " `ts_ms` " +
                "from topic_db " +
                "where `source`['table']='base_category2'");
        tableEnv.createTemporaryView("base_category2 ", c2table);
//        c2table.execute().print();

        Table c1table = tableEnv.sqlQuery("select " +
                " `after`['id'] id, " +
                " `after`['name'] name, " +
                " `op`," +
                " `ts_ms` " +
                "from topic_db " +
                "where `source`['table']='base_category1'");
        tableEnv.createTemporaryView("base_category1 ", c1table);
//        c1table.execute().print();


        Table Cptable = tableEnv.sqlQuery("SELECT\n" +
                "    u.id AS user_id,\n" +
                "    TO_CHAR(CONVERT(u.birthday,DATE),'yyyy-MM-dd') AS birthday,\n" +
                "    c1.name AS category1_name\n" +
                "FROM\n" +
                "    user_info u\n" +
                "JOIN order_info o ON u.id = o.user_id\n" +
                "JOIN order_detail od ON o.id = od.order_id\n" +
                "JOIN sku_info sku ON od.sku_id = sku.id\n" +
                "JOIN base_category3 c3 ON sku.category3_id = c3.id\n" +
                "JOIN base_category2 c2 ON c3.category2_id = c2.id\n" +
                "JOIN base_category1 c1 ON c2.category1_id = c1.id\n" +
                "GROUP BY u.id, c1.name,TO_CHAR(CONVERT(u.birthday,DATE),'yyyy-MM-dd');");
        Cptable.execute().print();



    }
}
