package com.youzan.fast.dump;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.youzan.fast.dump.util.QueryParserHelper;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.ArrayUtil;

import java.nio.file.Paths;
import java.util.List;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.03.25
 */
public class Test {
    public static void main(String[] args) throws Exception{
        IndexReader reader1 = DirectoryReader.open(FSDirectory.open(Paths.get("/Users/huafei/工作目录/tools/elasticsearch-5.6.14-slaver-2/data/nodes/0/indices/QNfez0_6TS6ZxDkUUWQcAA/0/index")));
        IndexSearcher searcher = new IndexSearcher(reader1);


        //TopDocs topDocs = searcher.search(new TermQuery(new Term("order_id","1547433904760978373")),10);
        String a = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": {\n" +
                "        \"term\": {\n" +
                "          \"kkk\": \"sfds62462\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"must_not\": {\n" +
                "        \"term\": {\n" +
                "          \"_uid\": \"data#AXE61RnCkCBsI-kIrEf9\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"should\": [\n" +
                "        {\n" +
                "          \"term\": {\n" +
                "            \"kkk\": \"sfds62462\"\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"term\": {\n" +
                "            \"kkk\": \"sfds62461\"\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"minimum_should_match\": 1,\n" +
                "      \"boost\": 1\n" +
                "    }\n" +
                "  }\n" +
                "}";
        JSONObject queryJson = JSON.parseObject(a);
        Query b = QueryParserHelper.parser(queryJson.getJSONObject("query"));

        System.out.println(Integer.MAX_VALUE);
        System.out.println(ArrayUtil.MAX_ARRAY_LENGTH);

        //TopDocs topDocs = searcher.search(LongPoint.newRangeQuery("order_id", 0L, Long.MAX_VALUE), Integer.MAX_VALUE);
        //BooleanQuery booleanQuery = new BooleanQuery.Builder().add(new BooleanClause(new TermQuery(new Term("order_id","1547433904760978373")),
        //        BooleanClause.Occur.MUST_NOT)).build();
        TopDocs scoreDocs = searcher.search(b, Integer.MAX_VALUE);
        for (ScoreDoc scoreDoc : scoreDocs.scoreDocs) {
            System.out.println(scoreDoc.doc);
        }
    }
}
