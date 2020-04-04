package com.youzan.fast.dump.common.reader;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.youzan.fast.dump.common.StatusEnum;
import com.youzan.fast.dump.common.rules.Rule;
import com.youzan.fast.dump.plugins.FastReindexTask;
import com.youzan.fast.dump.resolver.DataResolve;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author :  43333
 * @Project Name :  mainline
 * @Package Name :  com.youzan.clouddb.tools.general.common
 * @Description :  TODO
 * @Creation Date:  2018-08-16 10:07
 * --------  ---------  --------------------------
 */
public class LuceneFileReader extends AbstractFileReader {

    public LuceneFileReader(List<String> files, FastReindexTask task) {
        super(files, task);
    }

    @Override
    public void resolveFile(String file, DataResolve dataResolve, ExecutorService pool) throws Exception {

        String[] indexTypeFile = file.split(":");
        FileReadStatus fileReadStatus = new FileReadStatus(indexTypeFile[2], 0, 0);
        StandardDirectoryReader reader = null;
        try {
            fileReadList.add(fileReadStatus);
            reader = (StandardDirectoryReader) DirectoryReader.open(FSDirectory.open(Paths.get(indexTypeFile[2])));
            fileReadStatus.setTotalCount(reader.maxDoc());
            List<LeafReaderContext> readList = reader.getContext().leaves();
            int[] starts = new int[readList.size() + 1];
            int maxDoc = 0;
            for (int i = 0; i < readList.size(); i++) {
                starts[i] = maxDoc;
                IndexReader r = readList.get(i).reader();
                maxDoc += (long) r.maxDoc();
            }
            starts[readList.size()] = Math.toIntExact(maxDoc);
            int intervalSize = maxDoc / OneFileThreadNum;
            int start = 0;
            List<Future> futures = new ArrayList<>();
            for (int i = 0; i < OneFileThreadNum; i++) {
                futures.add(pool.submit(new DealDocument(start, start + intervalSize, starts, readList,
                        dataResolve, fileReadStatus, indexTypeFile[0], indexTypeFile[1], indexTypeFile[3])));
                start = start + intervalSize;
            }
            if (start < maxDoc) {
                futures.add(pool.submit(new DealDocument(start, maxDoc, starts, readList,
                        dataResolve, fileReadStatus, indexTypeFile[0], indexTypeFile[1], indexTypeFile[3])));
            }
            pool.shutdown();
            for (Future future : futures) {
                future.get();
            }
            fileReadStatus.setStatus(StatusEnum.SUCCESS.getStatus());
        } catch (Exception e) {
            fileReadStatus.setStatus(StatusEnum.FAILED.getStatus());
            fileReadStatus.setMsg(e.toString());
            pool.shutdownNow();
            throw e;
        } finally {
            if (null != reader) {
                try {
                    reader.close();
                } catch (IOException e) {
                    LOGGER.error("close reader error.", e);
                }
            }
        }
    }

    class DealDocument implements Callable<String> {
        private List<LeafReaderContext> readList;
        private DataResolve dataResolve;
        private FileReadStatus fileReadStatus;
        private int start;
        private int end;
        private int[] starts;
        private String index;
        private String type;
        private String sourceIndex;


        public DealDocument(int start, int end, int starts[],
                            List<LeafReaderContext> readList,
                            DataResolve dataResolve, FileReadStatus fileReadStatus,
                            String index, String type, String sourceIndex) {
            this.readList = readList;
            this.start = start;
            this.end = end;
            this.starts = starts;
            this.dataResolve = dataResolve;
            this.fileReadStatus = fileReadStatus;
            this.index = index;
            this.type = type;
            this.sourceIndex = sourceIndex;
        }

        @Override
        public String call() throws Exception {
            List<JSONObject> records = new ArrayList<>(batchSize);
            long currentCount = 0;
            for (int i = start; i < end; i++) {
                currentCount++;
                int subIndex = ReaderUtil.subIndex(i, starts);
                LeafReader reader = readList.get(subIndex).reader();
                int docId = i - this.starts[subIndex];
                Bits liveDocs = reader.getLiveDocs();
                //非存活的doc过滤掉
                if (null != liveDocs && !liveDocs.get(docId)) continue;

                String[] typeAndId = new String[2];
                Document document = reader.document(docId);

                //对于嵌套类文档，是不存储的，将其过滤掉
                if (0 == document.getFields().size()) {
                    continue;
                }

                //6.x和5.x底层存储结构不一样，兼容判断
                if (null == document.getField("_uid")) {
                    throw new Exception("can not find uid");
                    //typeAndId[0] = type;
                    //typeAndId[1] = Uid.decodeId(document.getField("_id").binaryValue().bytes);
                } else {
                    //同时兼容多type的情况
                    String uid = document.getField("_uid").stringValue();
                    typeAndId[0] = uid.split("#", -1)[0];
                    typeAndId[1] = uid.substring(typeAndId[0].length() + 1);
                }
                /*if (!"all".equals(type) && !typeAndId[0].equals(type)) {
                    continue;
                }*/

                JSONObject source = (JSONObject) JSON.parse(new String(document.getField("_source").binaryValue().bytes),
                        Feature.config(JSON.DEFAULT_PARSER_FEATURE, Feature.UseBigDecimal, false));


                JSONObject record = new JSONObject();

                IndexableField route = document.getField("_routing");
                if (null != route) {
                    record.put("route", route.stringValue());
                } else {
                    record.put("route", null);
                }

                //如果mode是update的话加上version
                if (isUpdateMode) {
                    NumericDocValues numericDocValues = reader.getNumericDocValues("_version");
                    //numericDocValues.advanceExact(docId);
                    //record.put("version", numericDocValues.longValue());
                    record.put("version", numericDocValues.get(docId));
                }

                record.put("type", typeAndId[0]);
                record.put("source", source);
                record.put("_id", typeAndId[1]);
                record.put("sourceIndex", sourceIndex);

                //自定义规则路由到对应索引或者转换record
                Set indexSet = new HashSet<String>();
                record.put("index", indexSet);
                if (isCustomType) {
                    for (Rule rule : ruleList) {
                        rule.transform(record);
                    }
                    if (0 == indexSet.size()) {
                        indexSet.add(index);
                    }
                } else {
                    indexSet.add(index);
                }


                records.add(record);
                if (records.size() % batchSize == 0) {
                    dataResolve.resolve(records);
                    fileReadStatus.setCurrentCount(fileReadStatus.getCurrentCount() + currentCount);
                    currentCount = 0;
                    records.clear();
                }
            }
            if (0 < records.size()) {
                dataResolve.resolve(records);
            }
            fileReadStatus.setCurrentCount(fileReadStatus.getCurrentCount() + currentCount);
            dataResolve.commit();
            return null;
        }
    }
}
