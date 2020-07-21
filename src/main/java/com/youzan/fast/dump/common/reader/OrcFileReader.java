package com.youzan.fast.dump.common.reader;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.youzan.fast.dump.client.HdfsConfClient;
import com.youzan.fast.dump.common.StatusEnum;
import com.youzan.fast.dump.common.rules.Rule;
import com.youzan.fast.dump.plugins.FastReindexRequest;
import com.youzan.fast.dump.plugins.FastReindexTask;
import com.youzan.fast.dump.resolver.DataResolve;
import lombok.NonNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.omg.CORBA.portable.ValueOutputStream;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2019.11.20
 */
public class OrcFileReader extends AbstractFileReader {

    private Configuration conf;

    //主键
    @NonNull
    private String primaryKey;

    //需要导的字段
    @NonNull
    private Set<String> needFields;

    //是nest类型的字段
    private Set<String> nestFields = new HashSet<>();

    public OrcFileReader(List<String> files, FastReindexTask task, FastReindexRequest.FastReindexRemoteInfo sourceInfo) throws Exception {
        super(files, task);
        conf = new HdfsConfClient(sourceInfo).getClient();
    }

    public OrcFileReader setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
        return this;
    }

    public OrcFileReader setNeedFields(String fields) {
        if (null != fields) {
            this.needFields = Sets.newHashSet(fields.split(","));
        }
        return this;
    }

    public OrcFileReader setNestFields(String nestFields) {
        if (null != nestFields) {
            this.nestFields = Sets.newHashSet(nestFields.split(","));
        }
        return this;
    }

    @Override
    public void resolveFile(String file, DataResolve dataResolve, ExecutorService pool) throws Exception {
        String[] targetAndFile = file.split(":");
        FileReadStatus fileReadStatus = new FileReadStatus(targetAndFile[2], 0, 0);
        AccessController.doPrivileged(
                (PrivilegedAction<Void>) () -> {
                    try {
                        fileReadList.add(fileReadStatus);
                        Reader reader = OrcFile.createReader(new Path(targetAndFile[2]),
                                OrcFile.readerOptions(conf));
                        fileReadStatus.setTotalCount(reader.getNumberOfRows());
                        List<String> fieldNames = reader.getSchema().getFieldNames();
                        Map<String, Integer> fieldIndexMap = new LinkedHashMap<>();
                        for (int i = 0; i < fieldNames.size(); i++) {
                            if (needFields.contains(fieldNames.get(i))) {
                                fieldIndexMap.put(fieldNames.get(i), i);
                            }
                        }
                        List<Future> futures = new ArrayList<>();
                        //hive这边单文件暂时就1个线程
                        for (int i = 0; i < 1; i++) {
                            futures.add(pool.submit(new DealDocument(reader, dataResolve, fileReadStatus,
                                    targetAndFile[0], targetAndFile[1], fieldIndexMap)));
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
                        throw new RuntimeException(e);
                    }
                    return null;
                });


    }

    class DealDocument implements Callable<Void> {
        private Reader reader;
        private DataResolve dataResolve;
        private FileReadStatus fileReadStatus;
        private String targetStorage;
        private String targetStorageType;
        private Map<String, Integer> fieldIndexMap;

        public DealDocument(Reader reader,
                            DataResolve dataResolve, FileReadStatus fileReadStatus,
                            String targetStorage, String targetStorageType, Map<String, Integer> fieldIndexMap) {
            this.reader = reader;
            this.dataResolve = dataResolve;
            this.fileReadStatus = fileReadStatus;
            this.targetStorage = targetStorage;
            this.targetStorageType = targetStorageType;
            this.fieldIndexMap = fieldIndexMap;
        }

        @SuppressWarnings("all")
        @Override
        public Void call() throws Exception {
            List<JSONObject> records = new ArrayList<>(batchSize);
            long currentCount = 0;
            RecordReader rows = reader.rows();
            VectorizedRowBatch batch = reader.getSchema().createRowBatch();
            while (rows.nextBatch(batch)) {
                for (int r = 0; r < batch.size; ++r) {
                    JSONObject row = new JSONObject();
                    currentCount++;
                    JSONObject source = new JSONObject();
                    row.put("type", targetStorageType);
                    transform(batch, r, source);
                    row.put("source", source);
                    row.put("_id", source.get(primaryKey));
                    source.remove(primaryKey);
                    //自定义规则路由到对应索引或者转换record
                    Set indexSet = new HashSet<String>();
                    row.put("index", indexSet);
                    if (isCustomType) {
                        for (Rule rule : ruleList) {
                            rule.transform(row);
                        }
                        if (0 == indexSet.size()) {
                            indexSet.add(targetStorage);
                        }
                    } else {
                        indexSet.add(targetStorage);
                    }
                    records.add(row);
                    if (records.size() % batchSize == 0) {
                        dataResolve.resolve(records);
                        fileReadStatus.setCurrentCount(fileReadStatus.getCurrentCount() + currentCount);
                        currentCount = 0;
                        records.clear();
                    }
                }
            }

            if (0 < records.size()) {
                dataResolve.resolve(records);
            }
            fileReadStatus.setCurrentCount(fileReadStatus.getCurrentCount() + currentCount);
            dataResolve.commit();
            return null;
        }

        private void transform(VectorizedRowBatch batch, int r, JSONObject source) {
            for (String key : fieldIndexMap.keySet()) {
                ColumnVector cv = batch.cols[fieldIndexMap.get(key)];
                if (cv.isNull[r]) {
                    continue;
                }
                Object value = getValue(r, cv);
                if (nestFields.contains(key)) {
                    value = JSONArray.parseArray(value.toString());
                }
                source.put(key, value);
            }
        }


        /**
         * 根据CV的类型来获取相应的值
         *
         * @param r  下标
         * @param cv 栏目
         * @return 值
         */
        private Object getValue(int r, ColumnVector cv) {
            Object value = null;
            if (cv instanceof BytesColumnVector) {
                value = ((BytesColumnVector) cv).toString(r);
            } else if (cv instanceof LongColumnVector) {
                value = ((LongColumnVector) cv).vector[r];
            } else if (cv instanceof DoubleColumnVector) {
                value = ((DoubleColumnVector) cv).vector[r];
            } else if (cv instanceof ListColumnVector) {
                ListColumnVector listCv = (ListColumnVector) cv;
                List<Object> listValue = new ArrayList<>();
                for (int i = (int) listCv.offsets[r]; i < listCv.offsets[r] + listCv.lengths[r]; i++) {
                    listValue.add(getValue(i, listCv.child));
                }
                value = listValue;
            }
            return value;
        }
    }
}
