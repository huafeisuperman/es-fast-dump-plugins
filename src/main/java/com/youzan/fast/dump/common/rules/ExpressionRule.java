package com.youzan.fast.dump.common.rules;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.jexl3.*;
import org.apache.commons.jexl3.internal.Engine;

import java.util.Map;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2019.07.17
 */
public class ExpressionRule extends AbstractEsRule implements Rule {

    private String expression;

    private String targetIndex;

    private JexlScript ex;

    public ExpressionRule(String field, String rules) {
        super(field, rules);
    }


    @Override
    protected void parseRuleMap(Map<String, String> ruleMap) {
        //表达式这种的话只有一个对应关系
        ruleMap.forEach((x, y) -> {
            expression = x;
            ex = new Engine().createScript(expression);
            targetIndex = y;
        });
    }

    @Override
    protected String generateEsIndex(Object value) {
        return targetIndex;
    }

    @Override
    protected void transformSource(JSONObject record) {
        JexlContext jc = new MapContext();
        jc.set(field, record);
        jc.set("Integer",Integer.class);
        ex.execute(jc);
    }
}
