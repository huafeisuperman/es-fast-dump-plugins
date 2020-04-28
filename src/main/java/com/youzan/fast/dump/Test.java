package com.youzan.fast.dump;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.youzan.fast.dump.util.QueryParserHelper;
import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.ArrayUtil;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.03.25
 */
public class Test {
    public static void main(String[] args) throws Exception{
        IndexReader reader1 = DirectoryReader.open(FSDirectory.open(Paths.get("/Users/huafei/工作目录/tools/elasticsearch-5.6.14-slaver-2/data/nodes/0/indices/Ug7P6w-aR1SImUTiKVdc3A/0/index")));
        IndexSearcher searcher = new IndexSearcher(reader1);

        System.out.println(reader1.maxDoc());


        //TopDocs topDocs = searcher.search(new TermQuery(new Term("order_id","1547433904760978373")),10);
        String a = "{\"query\": {\n" +
                "      \"bool\": {\n" +
                "        \"must\": [\n" +
                "          {\n" +
                "            \"term\": {\n" +
                "              \"sex\": false\n" +
                "            }\n" +
                "          }\n" +
                "        ]\n" +
                "      }\n" +
                "    }}";
        System.out.println(a);

        JSONObject queryJson = (JSONObject) JSON.parse(a,Feature.config(JSON.DEFAULT_PARSER_FEATURE, Feature.UseBigDecimal, false));
        Map<String,String> map = new HashMap<>();
        map.put("sex","boolean");
        Query b = QueryParserHelper.parser(queryJson.getJSONObject("query"), map);
        System.out.println(b);


        //System.out.println(Integer.MAX_VALUE);
        //System.out.println(ArrayUtil.MAX_ARRAY_LENGTH);

        //TopDocs topDocs = searcher.search(LongPoint.newRangeQuery("order_id", 0L, Long.MAX_VALUE), Integer.MAX_VALUE);
        //BooleanQuery booleanQuery = new BooleanQuery.Builder().add(new BooleanClause(new TermQuery(new Term("order_id","1547433904760978373")),
        //        BooleanClause.Occur.MUST_NOT)).build();
        TopDocs scoreDocs = searcher.search(b, Integer.MAX_VALUE);
        System.out.println(scoreDocs.totalHits);

        /*for (ScoreDoc scoreDoc : scoreDocs.scoreDocs) {
            System.out.println(scoreDoc.doc);
        }*/
        long startTime = System.currentTimeMillis();
        for (int i=0;i<1000000000;i++) {
            JSONObject pp = new JSONObject();
            pp.put("a","sdfsd");
            Object c = pp.get("a");

        }
        System.out.println(System.currentTimeMillis()-startTime);

       /* GroovyClassLoader classLoader = new GroovyClassLoader();
        Class groovyClass = classLoader.parseClass("str = name+age;return str");
        for (int i=0;i<1000;i++) {
            Binding binding = new Binding();
            binding.setVariable("name", "iamzhongyong");
            binding.setVariable("age", i);
            //InvokerHelper(script, function, objects);
            Script script = (Script) groovyClass.newInstance();
            script.setBinding(binding);
            System.out.println(script.run());
        }*/
    }
}
