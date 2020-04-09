package com.youzan.fast.dump.common.rules;

import com.alibaba.fastjson.JSONObject;
import com.youzan.fast.dump.common.BaseLogger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2019.07.16
 */
public abstract class AbstractEsRule implements Rule, BaseLogger {

    protected String field;

    protected String rules;

    public AbstractEsRule(String field, String rules) {
        this.field = field;
        this.rules = rules;
        parseRules();
    }

    @Override
    public void transform(JSONObject record) throws Exception {
        JSONObject source = record.getJSONObject("source");
        String index = generateEsIndex(source.get(field));
        if (!index.equals("default")) {
            ((Set) record.get("index")).add(index);
        }
        transformSource(record);
    }

    @Override
    public void parseRules() {
        Map<String, String> ruleMap = new HashMap<>();
        //为了区分表达式中含有逗号这种情况
        if (1 == rules.split("->").length) {
            String[] keyValues = rules.split("->");
            ruleMap.put(keyValues[0], "default");
        } else {
            for (String rule : rules.split(",")) {
                String[] keyValues = rule.split("->");
                ruleMap.put(keyValues[0], keyValues[1]);
            }
        }

        parseRuleMap(ruleMap);
    }

    protected abstract void parseRuleMap(Map<String, String> ruleMap);

    protected abstract String generateEsIndex(Object value);

    protected abstract void transformSource(JSONObject record);
}
