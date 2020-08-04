package com.youzan.fast.dump.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.04.06
 */
public class QueryParserHelper {

    public static final String TERM = "term";
    public static final String RANGE = "range";
    public static final String TERMS = "terms";
    public static final String BOOL = "bool";
    public static final String MUST = "MUST";
    public static final String FILTER = "FILTER";
    public static final String SHOULD = "SHOULD";
    public static final String MUST_NOT = "MUST_NOT";
    public static final String MINIMUM_SHOULD_MARCH = "minimum_should_match";

    public static Query parser(JSONObject queryJson) {
        return parser(queryJson, new HashMap<>());
    }

    public static Query parser(JSONObject queryJson, Map<String, String> fieldInfo) {
        return new ConstantScoreQuery(buildQuery(queryJson, fieldInfo));
    }

    private static Query buildQuery(JSONObject queryJson, Map<String, String> fieldInfo) {
        Map<String, Object> map = queryJson.getInnerMap();
        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getKey().equals(TERM)) {
                Map.Entry<String, Object> value = ((Map<String, Object>) entry.getValue()).entrySet().iterator().next();
                booleanQueryBuilder.add(new BooleanClause(castTermToQueryByType(fieldInfo.get(value.getKey()), value.getKey(), value.getValue()),
                        BooleanClause.Occur.MUST));
            } else if (entry.getKey().equals(RANGE)) {
                Map.Entry<String, Object> value = ((Map<String, Object>) entry.getValue()).entrySet().iterator().next();
                JSONObject rangeJs = (JSONObject) value.getValue();
                Object lowValue = rangeJs.get("gte") == null ? rangeJs.get("gt") : rangeJs.get("gte");
                Object highValue = rangeJs.get("lte") == null ? rangeJs.get("lt") : rangeJs.get("lte");
                booleanQueryBuilder.add(new BooleanClause(castRangeToQueryByType(fieldInfo.get(value.getKey()),
                        value.getKey(), lowValue, highValue),
                        BooleanClause.Occur.MUST));
            } else if (entry.getKey().equals(TERMS)) {
                Map.Entry<String, Object> value = ((Map<String, Object>) entry.getValue()).entrySet().iterator().next();
                List<BytesRef> terms = new ArrayList<>();
                for (Object o : ((List) value.getValue())) {
                    terms.add(new BytesRef(o.toString()));
                }
                booleanQueryBuilder.add(new BooleanClause(new TermInSetQuery(value.getKey(), terms),
                        BooleanClause.Occur.MUST));
            } else if (entry.getKey().equals(BOOL)) {
                Map<String, Object> value = (Map<String, Object>) entry.getValue();
                for (Map.Entry<String, Object> boolEntry : value.entrySet()) {
                    BooleanClause.Occur occur = null;
                    switch (boolEntry.getKey().toUpperCase()) {
                        case MUST:
                            occur = BooleanClause.Occur.MUST;
                            break;
                        case FILTER:
                            occur = BooleanClause.Occur.FILTER;
                            break;
                        case SHOULD:
                            occur = BooleanClause.Occur.SHOULD;
                            break;
                        case MUST_NOT:
                            occur = BooleanClause.Occur.MUST_NOT;
                            break;
                        case MINIMUM_SHOULD_MARCH:
                            booleanQueryBuilder.setMinimumNumberShouldMatch((int) boolEntry.getValue());

                    }
                    if (null != occur) {
                        if (boolEntry.getValue() instanceof JSONObject) {
                            booleanQueryBuilder.add(parser((JSONObject) boolEntry.getValue(), fieldInfo), occur);
                        } else {
                            for (Object o : ((List) boolEntry.getValue())) {
                                booleanQueryBuilder.add(parser((JSONObject) o, fieldInfo), occur).build();
                            }
                        }
                    }


                }

            } else {
                throw new RuntimeException("not support query");
            }
        }
        return booleanQueryBuilder.build();
    }


    private static Query castTermToQueryByType(String type, String key, Object value) {
        if (type == null) {
            return new TermQuery(new Term(key, value.toString()));
        }
        Query query;
        switch (type) {
            case "double":
                query = DoublePoint.newExactQuery(key, Double.parseDouble(value.toString()));
                break;
            case "integer":
                query = IntPoint.newExactQuery(key, Integer.parseInt(value.toString()));
                break;
            case "long":
                query = LongPoint.newExactQuery(key, Long.parseLong(value.toString()));
                break;
            case "boolean":
                String v = "F";
                if ((boolean) value) {
                    v = "T";
                }
                query = new TermQuery(new Term(key, v));
                break;
            default:
                query = new TermQuery(new Term(key, value.toString()));

        }
        return query;

    }

    private static Query castRangeToQueryByType(String type, String key, Object lowValue, Object highValue) {
        if (type == null) {
            return new TermRangeQuery(key, new BytesRef(lowValue.toString().getBytes()), new BytesRef(highValue.toString().getBytes()), true, true);
        }
        Query query;
        switch (type) {
            case "double":
                query = DoublePoint.newRangeQuery(key, Double.parseDouble(lowValue.toString()), Double.parseDouble(highValue.toString()));
                break;
            case "integer":
                query = IntPoint.newRangeQuery(key, Integer.parseInt(lowValue.toString()), Integer.parseInt(highValue.toString()));
                break;
            case "long":
                query = LongPoint.newRangeQuery(key, Long.parseLong(lowValue.toString()), Long.parseLong(highValue.toString()));
                break;
            default:
                query = new TermRangeQuery(key, new BytesRef(lowValue.toString().getBytes()), new BytesRef(highValue.toString().getBytes()), true, true);

        }
        return query;
    }
}
