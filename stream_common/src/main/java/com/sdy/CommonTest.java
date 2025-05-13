package com.sdy;


import com.sdy.utils.ConfigUtils;

/**
 * @Package com.stream.common.CommonTest
 * @Author danyu-shi
 * @Date 2025/5/12 9:52
 * @description: Test
 */
public class CommonTest {

    public static void main(String[] args) {
        String kafka_err_log = ConfigUtils.getString("kafka.err.log");
        System.err.println(kafka_err_log);
    }


}
