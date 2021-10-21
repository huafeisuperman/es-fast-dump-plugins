package com.youzan.fast.dump.resolver;

import com.alibaba.fastjson.JSONObject;
import com.youzan.fast.dump.client.ESTransportClient;
import com.youzan.fast.dump.common.IndexModeEnum;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.VersionConflictEngineException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author :  43333
 * @Project Name :  mainline
 * @Package Name :  com.youzan.clouddb.tools.general.resolver
 * @Description :  TODO
 * @Creation Date:  2018-08-16 10:08
 * --------  ---------  --------------------------
 */
public class ESDataResolve extends BaseDataResolve {

    private Client esClient;

    private IndexModeEnum mode;

    public ESDataResolve(Client client, String mode, int speedLimit) throws Exception {
        super(speedLimit);
        esClient = new ESTransportClient(client).getClient();
        this.mode = IndexModeEnum.findModeEnum(mode);
    }

    @Override
    public void resolve(List<JSONObject> records) throws Exception {
        long oneBatchStartTime = System.currentTimeMillis();
        BulkRequestBuilder bulkRequest = esClient.prepareBulk();
        for (JSONObject record : records) {
            Map<String, ?> dataMap = record.getJSONObject("source").getInnerMap();
            String sourceType = record.get("type").toString();
            String id = record.get("_id").toString();
            String route = record.get("route") == null ? null : record.get("route").toString();
            ((Set) record.get("index")).forEach(index -> {
                IndexRequestBuilder indexRequest = esClient.prepareIndex(index.toString(), sourceType, id).setSource(dataMap).setRouting(route);
                //按照模式来区分不同的索引类型
                if (mode == IndexModeEnum.CREATE) {
                    indexRequest.setCreate(true);
                } else if (mode == IndexModeEnum.UPDATE) {
                    indexRequest.setVersion(record.getLong("version"));
                    indexRequest.setVersionType(VersionType.EXTERNAL_GTE);
                }
                bulkRequest.add(indexRequest);
            });

        }
        if (bulkRequest.numberOfActions() > 0) {
            rateLimiter.acquire(bulkRequest.numberOfActions());
            BulkResponse responses = bulkRequest.get();
            int retryTimes = 0;
            while (hasFailures(responses)) {
                if (retryTimes < 2) {
                    Thread.sleep(5000L);
                    responses = bulkRequest.get();
                    retryTimes++;
                } else {
                    break;
                }
            }
            if (hasFailures(responses)) {
                throw new Exception("bulk error" + responses.buildFailureMessage());
            }
            long oneBatchEndTime = System.currentTimeMillis();
            if (oneBatchEndTime - oneBatchStartTime > 5000L)
                LOGGER.info("one batch cost:" + (oneBatchEndTime - oneBatchStartTime));
        }
    }

    @Override
    public void close() throws Exception {
        esClient.close();
    }

    private boolean hasFailures(BulkResponse responses) {
        for (BulkItemResponse itemResponse : responses.getItems()) {
            if (itemResponse.isFailed() &&
                    itemResponse.getFailure().getCause() instanceof VersionConflictEngineException) {
                continue;
            } else {
                if (itemResponse.isFailed()) {
                    return true;
                }
            }
        }
        return false;
    }
}
