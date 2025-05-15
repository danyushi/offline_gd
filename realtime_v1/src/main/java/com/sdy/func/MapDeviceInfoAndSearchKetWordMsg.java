package com.sdy.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @Package com.sdy.func.MapDeviceInfoAndSearchKetWordMsg
 * @Author danyu-shi
 * @Date 2025/5/13 16:18
 * @description:处理设备信息和搜索关键词的提取
 */
public class MapDeviceInfoAndSearchKetWordMsg extends RichMapFunction<JSONObject, JSONObject> {


    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        //提取公共信息
        JSONObject result = new JSONObject();
        if (jsonObject.containsKey("common")){
            //提取搜索关键词
            JSONObject common = jsonObject.getJSONObject("common");
            result.put("uid",common.getString("uid") != null ? common.getString("uid") : "-1");
            result.put("ts",jsonObject.getLongValue("ts"));
            JSONObject deviceInfo = new JSONObject();
            common.remove("sid");
            common.remove("mid");
            common.remove("is_new");
            deviceInfo.putAll(common);
            result.put("deviceInfo",deviceInfo);
            if(jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()){
                JSONObject pageInfo = jsonObject.getJSONObject("page");
                if (pageInfo.containsKey("item_type") && pageInfo.getString("item_type").equals("keyword")){
                    String item = pageInfo.getString("item");
                    result.put("search_item",item);
                }
            }
        }
        //处理操作系统字段
        JSONObject deviceInfo = result.getJSONObject("deviceInfo");
        String os = deviceInfo.getString("os").split(" ")[0];
        deviceInfo.put("os",os);


        return result;
    }
}