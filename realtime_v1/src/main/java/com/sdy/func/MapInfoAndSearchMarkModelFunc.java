package com.sdy.func;

import com.alibaba.fastjson.JSONObject;
import com.sdy.domain.Constant;
import com.sdy.domin.DimBaseCategory2;
import com.sdy.domin.DimCategoryCompare;
import com.sdy.utils.JdbcUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Package com.retailersv1.func.MapDeviceMarkModel
 * @Author dnayu-shi
 * @Date 2025/5/13 21:34
 * @description: 设备打分模型
 */
public class MapInfoAndSearchMarkModelFunc extends RichMapFunction<JSONObject,JSONObject> {

    private final double deviceRate;
    private final Map<String, DimBaseCategory2> categoryMap;
    private List<DimCategoryCompare> dimCategoryCompares2;
    private Connection connection;

    public MapInfoAndSearchMarkModelFunc(List<DimBaseCategory2> dimBaseCategories, double deviceRate, double searchRate) {
        this.deviceRate = deviceRate;
        this.categoryMap = new HashMap<>();
        // 将 DimBaseCategory 对象存储到 Map中  加快查询
        for (DimBaseCategory2 category : dimBaseCategories) {
            categoryMap.put(category.getBcname(), category);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = JdbcUtils.getMySQLConnection(
                Constant.MYSQL_URL,
                Constant.MYSQL_USER_NAME,
                Constant.MYSQL_PASSWORD
        );
        String sql = "select id, total_amount, create_time,user_id from gmall_v1_danyu_shi.order_info;";
        dimCategoryCompares2 = JdbcUtils.queryList2(connection, sql, DimCategoryCompare.class, true);
        super.open(parameters);
    }



    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        String os = jsonObject.getString("os");
        String[] labels = os.split(",");
        String judge_os = labels[0];
        jsonObject.put("judge_os", judge_os);

            jsonObject.put("device_18_24", round(0.7 * deviceRate));
            jsonObject.put("device_25_29", round(0.6 * deviceRate));
            jsonObject.put("device_30_34", round(0.5 * deviceRate));
            jsonObject.put("device_35_39", round(0.4 * deviceRate));
            jsonObject.put("device_40_49", round(0.3 * deviceRate));
            jsonObject.put("device_50",    round(0.2 * deviceRate));



        return jsonObject;

    }

    private static double round(double value) {
        return BigDecimal.valueOf(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }


    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}
