package com.youzan.fast.dump.common.rules;

import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2019.07.16
 */
public class TimeStampRule extends AbstractEsRule implements Rule {

    private static long MAX_LONG = Long.MAX_VALUE;

    private List<TimeRange> rangeList;

    public TimeStampRule(String field, String rules) {
        super(field, rules);
    }

    @Override
    protected void parseRuleMap(Map<String, String> ruleMap) {
        rangeList = new ArrayList<>();
        ruleMap.forEach((x, y) -> {
            String[] range = x.split("~");
            TimeRange timeRange = new TimeRange();
            timeRange.setLowTime(range[0].equals("null") ? -1 : Long.parseLong(range[0]));
            timeRange.setHighTime(range[1].equals("null") ? MAX_LONG : Long.parseLong(range[1]));
            timeRange.setIndex(y);
            rangeList.add(timeRange);
        });
    }

    @Override
    protected String generateEsIndex(Object value) {
        for (TimeRange entity : rangeList) {
            if (entity.isContain(Long.parseLong(value.toString()))) {
                return entity.getIndex();
            }
        }
        throw new RuntimeException(String.format("can not find timeRange for %s", value));
    }

    @Override
    protected void transformSource(JSONObject record) {

    }

    public class TimeRange {

        private long lowTime;

        private long highTime;

        private String index;

        public TimeRange() {

        }

        public TimeRange(long lowTime, long highTime, String index) {
            this.lowTime = lowTime;
            this.highTime = highTime;
            this.index = index;
        }

        public boolean isContain(long sourceTime) {
            if (sourceTime >= lowTime && sourceTime < highTime) {
                return true;
            }
            return false;
        }

        public void setLowTime(long lowTime) {
            this.lowTime = lowTime;
        }

        public void setHighTime(long highTime) {
            this.highTime = highTime;
        }

        public void setIndex(String index) {
            this.index = index;
        }

        public String getIndex() {
            return index;
        }
    }
}
