package com.sdy.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.eclipse.jetty.client.AuthenticationProtocolHandler.LOG;

/**
 * @Package com.retailersv1.func.AggregateUserDataProcessFunction
 * @Author danyi_shi
 * @Date 2025/5/13 16:28
 * @description:
 */
public class AggregateOrderInfoFunction extends KeyedProcessFunction<String, JSONObject,JSONObject> {

    private transient ValueState<Long> pvState;
    private transient MapState<String, Set<String>> fieldsState;


    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化PV状态
        pvState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pv-state", Long.class)
        );

        // 初始化字段集合状态（使用TypeHint保留泛型信息）
        MapStateDescriptor<String, Set<String>> fieldsDescriptor =
                new MapStateDescriptor<>(
                        "fields-state",
                        Types.STRING,
                        TypeInformation.of(new TypeHint<Set<String>>() {})
                );

        fieldsState = getRuntimeContext().getMapState(fieldsDescriptor);
    }


    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
        // 更新PV
        Long pv = pvState.value() == null ? 1L : pvState.value() + 1;
        pvState.update(pv);

        // 提取设备信息和搜索词
        JSONObject orderInfo = value.getJSONObject("orderInfo");
        String op = null;

        // 修正：判断 orderInfo 不为空且有内容
        if (orderInfo != null && !orderInfo.isEmpty()) { // 关键修改点！
            op = orderInfo.getString("op");
            String total_amount = orderInfo.getString("total_amount");
            String user_id = orderInfo.getString("user_id");
            String id = orderInfo.getString("id");
            String create_time = orderInfo.getString("create_time");
            String ts_ms = orderInfo.getString("ts_ms");
            String searchItem = value.containsKey("search_item") ? value.getString("search_item") : null;

            // 更新字段集合
            updateField("op", op);
            updateField("total_amount", total_amount);
            updateField("user_id", user_id);
            updateField("id", id);
            updateField("create_time", create_time);
            updateField("ts_ms", ts_ms);
            if (searchItem != null) {
                updateField("search_item", searchItem);
            }
        } else {
            LOG.warn("Invalid orderInfo: {}", orderInfo); // 记录异常情况
        }

        // 构建输出JSON
        JSONObject output = new JSONObject();
        output.put("user_id", value.getString("user_id"));
        output.put("op", op); // op 可能为 null
        output.put("ts_ms", String.join(",", getField("ts_ms")));
        output.put("total_amount", String.join(",", getField("total_amount")));
        output.put("create_time", String.join(",", getField("create_time")));
        output.put("id", String.join(",", getField("id")));
        output.put("search_item", String.join(",", getField("search_item")));

        // 确保输出的键非空（如果 op 被用作键）
        if (op != null) {
            out.collect(output);
        } else {
            LOG.warn("Dropping element with null op: {}", output);
        }
    }

    // 辅助方法：更新字段集合
    private void updateField(String field, String value) throws Exception {
        Set<String> set = fieldsState.get(field) == null ? new HashSet<>() : fieldsState.get(field);
        set.add(value);
        fieldsState.put(field, set);
    }

    // 辅助方法：获取字段集合
    private Set<String> getField(String field) throws Exception {
        return fieldsState.get(field) == null ? Collections.emptySet() : fieldsState.get(field);
    }


}
