package com.youzan.fast.dump.common.rules;

import com.alibaba.fastjson.JSONObject;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2019.07.16
 */
public interface Rule {


    void transform(JSONObject record) throws Exception;

    void parseRules();

}
