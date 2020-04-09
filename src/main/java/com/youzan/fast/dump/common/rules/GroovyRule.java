package com.youzan.fast.dump.common.rules;

import com.alibaba.fastjson.JSONObject;
import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.04.08
 */
public class GroovyRule extends AbstractEsRule implements Rule {

    private String scriptSource;

    private Class groovyClass;

    public GroovyRule(String field, String rules) {
        super(field, rules);
    }

    @Override
    protected void parseRuleMap(Map<String, String> ruleMap) {

        //表达式这种的话只有一个对应关系
        AccessController.doPrivileged(
                (PrivilegedAction<Void>) () -> {
                    ruleMap.forEach((x, y) -> {
                        scriptSource = x;
                        GroovyClassLoader classLoader = new GroovyClassLoader(getClass().getClassLoader());
                        groovyClass = classLoader.parseClass(scriptSource);
                    });
                    return null;
                });

    }

    @Override
    protected String generateEsIndex(Object value) {
        return "default";
    }

    @Override
    protected void transformSource(JSONObject record) {
        try {
            Script script = (Script) groovyClass.newInstance();
            Binding binding = new Binding();
            binding.setVariable(field, record);
            script.setBinding(binding);
            script.run();
        } catch (Exception e) {
            throw new RuntimeException("groovy script run error", e);
        }
    }
}
