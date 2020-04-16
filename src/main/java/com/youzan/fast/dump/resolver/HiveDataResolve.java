package com.youzan.fast.dump.resolver;

import com.alibaba.fastjson.JSONObject;
import com.youzan.fast.dump.client.HdfsConfClient;
import com.youzan.fast.dump.plugins.FastReindexRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2019.11.07
 */
public class HiveDataResolve extends BaseDataResolve {

    private Configuration conf;

    private TypeDescription schema;

    ThreadLocal<OrcBatchROW> writerThreadLocal = new ThreadLocal<>();

    public static final String ID_FIELD = "id";

    public static final String VERSION_FIELD = "version";

    public static final String INDEX_FIELD = "index";

    public static final String ALL_FIELD = "all_field";

    public HiveDataResolve(FastReindexRequest.FastReindexRemoteInfo remoteInfo, int speedLimit) throws Exception {
        super(speedLimit);
        conf = new HdfsConfClient(remoteInfo).getClient();
        schema = TypeDescription.createStruct();
        schema.addField(ID_FIELD, TypeDescription.createString())
                .addField(VERSION_FIELD, TypeDescription.createString())
                .addField(INDEX_FIELD, TypeDescription.createString())
                .addField(ALL_FIELD, TypeDescription.createString());
    }


    @Override
    public void resolve(List<JSONObject> data) throws Exception {
        AccessController.doPrivileged(
                (PrivilegedAction<Configuration>) () -> {
                    try {
                        OrcBatchROW orcBatchROW = writerThreadLocal.get();
                        if (orcBatchROW == null) {
                            orcBatchROW = initialWriter(((Set) (data.get(0).get("index"))).toArray()[0].toString());
                        }
                        VectorizedRowBatch batch = orcBatchROW.getBatch();
                        BytesColumnVector col1 = (BytesColumnVector) batch.cols[0];
                        BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];
                        BytesColumnVector col3 = (BytesColumnVector) batch.cols[2];
                        BytesColumnVector col4 = (BytesColumnVector) batch.cols[3];

                        for (JSONObject record : data) {
                            int row = batch.size++;
                            col1.setVal(row, record.getString("_id").getBytes());
                            col2.setVal(row, record.getLong("version").toString().getBytes());
                            col3.setVal(row, record.getString("sourceIndex").getBytes());
                            col4.setVal(row, new JSONObject(new TreeMap<>(record.getJSONObject("source").getInnerMap())).toString().getBytes());

                        }
                        rateLimiter.acquire(data.size());
                        orcBatchROW.getWriter().addRowBatch(batch);
                        batch.reset();
                        return null;
                    } catch (Exception e) {
                        throw new RuntimeException("write hive file error.", e);
                    }
                });
    }

    private synchronized OrcBatchROW initialWriter(String path) throws Exception {
        if (null == writerThreadLocal.get()) {
            OrcBatchROW orcBatchROW = new OrcBatchROW();
            orcBatchROW.setWriter(OrcFile.createWriter(new Path(path + "/" + UUID.randomUUID()),
                    OrcFile.writerOptions(conf)
                            .setSchema(schema)));
            orcBatchROW.setBatch(schema.createRowBatch());
            writerThreadLocal.set(orcBatchROW);
        }
        return writerThreadLocal.get();
    }

    @Override
    public void commit() throws Exception {
        AccessController.doPrivileged((PrivilegedAction<Configuration>) () -> {
            try {
                if (null != writerThreadLocal.get()) {
                    writerThreadLocal.get().getWriter().close();
                    writerThreadLocal.remove();
                }
                return null;
            } catch (IOException e) {
                throw new RuntimeException("commit file error.", e);
            }
        });

    }

    @Override
    public void close() throws Exception {
        writerThreadLocal.remove();
    }

    class OrcBatchROW {
        private Writer writer;
        private VectorizedRowBatch batch;

        public Writer getWriter() {
            return writer;
        }

        public void setWriter(Writer writer) {
            this.writer = writer;
        }

        public VectorizedRowBatch getBatch() {
            return batch;
        }

        public void setBatch(VectorizedRowBatch batch) {
            this.batch = batch;

        }
    }
}
