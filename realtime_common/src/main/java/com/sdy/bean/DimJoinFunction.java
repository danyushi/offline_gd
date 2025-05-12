package com.sdy.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * @author Felix
 * @date  2025/5/12 9:30
 * 维度关联需要实现的接口
 */
public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;
}
