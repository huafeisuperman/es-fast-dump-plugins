package com.youzan.fast.dump.common.rules;

import com.alibaba.fastjson.JSONObject;

import java.util.Map;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2019.07.16
 */
public class HashRule extends AbstractEsRule implements Rule {

    private Map<String, String> idToIndex;

    private int number;


    public HashRule(String field, String rules) {
        super(field, rules);
    }

    @Override
    protected void parseRuleMap(Map<String, String> ruleMap) {
        idToIndex = ruleMap;
        number = idToIndex.size();
    }

    @Override
    protected String generateEsIndex(Object value) {
        String hashId = String.valueOf(Long.parseLong(value.toString()) % number);
        String index = idToIndex.get(hashId);
        if (null == index) {
            throw new RuntimeException(String.format("value:%s id:%s can not find index", value, hashId));
        }
        return index;
    }

    @Override
    protected void transformSource(JSONObject record) {

    }
}
