package com.youzan.fast.dump.resolver;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.youzan.fast.dump.client.ESRestClient;
import com.youzan.fast.dump.common.IndexModeEnum;
import com.youzan.fast.dump.plugins.FastReindexRequest;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.index.VersionType;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.04.04
 */
public class ESRestDataResolve extends BaseDataResolve {

    private RestClient esClient;

    private IndexModeEnum mode;

    public ESRestDataResolve(FastReindexRequest.FastReindexRemoteInfo remoteInfo, String mode, int speedLimit) throws Exception {
        super(speedLimit);
        esClient = new ESRestClient(remoteInfo).getClient();
        this.mode = IndexModeEnum.findModeEnum(mode);
    }

    @Override
    public void resolve(List<JSONObject> records) throws Exception {
        AccessController.doPrivileged(
                (PrivilegedAction<Void>) () -> {
                    try {
                        long oneBatchStartTime = System.currentTimeMillis();
                        StringBuilder recordBuilder = new StringBuilder();
                        for (JSONObject record : records) {

                            JSONObject source = record.getJSONObject("source");
                            String sourceType = record.get("type").toString();
                            String id = record.get("_id").toString();
                            String route = record.get("route") == null ? null : record.get("route").toString();

                            ((Set) record.get("index")).forEach(index -> {
                                JSONObject jsonObject = new JSONObject();
                                JSONObject childJson = new JSONObject();
                                childJson.put("_id", id);
                                if (!sourceType.equals("_doc")) {
                                    childJson.put("_type", sourceType);
                                }
                                childJson.put("_index", index);
                                childJson.put("_route", route);
                                //按照模式来区分不同的索引类型
                                if (mode.equals(IndexModeEnum.CREATE)) {
                                    jsonObject.put("create", childJson);

                                } else if (mode.equals(IndexModeEnum.INSERT)) {
                                    jsonObject.put("index", childJson);

                                } else {
                                    jsonObject.put("index", childJson);
                                    if (null != record.getLong("version")) {
                                        childJson.put("version", record.getLong("version"));
                                        childJson.put("version_type", VersionType.EXTERNAL_GTE.toString().toLowerCase());
                                    }
                                }
                                recordBuilder.append(jsonObject.toJSONString()).append("\n");
                                recordBuilder.append(source).append("\n");
                            });

                        }
                        if (recordBuilder.length() > 0) {
                            rateLimiter.acquire(records.size());
                            HttpEntity entity = new NStringEntity(recordBuilder.toString(),
                                    ContentType.APPLICATION_JSON);
                            Request request = new Request("post", "/_bulk");
                            request.setEntity(entity);
                            Response response = esClient.performRequest(request);
                            String content = EntityUtils.toString(response.getEntity());
                            int retryTimes = 0;
                            while (hasFailures(content)) {
                                if (retryTimes < 2) {
                                    Thread.sleep(1000L);
                                    response = esClient.performRequest(request);
                                    content = EntityUtils.toString(response.getEntity());
                                    retryTimes++;
                                } else {
                                    break;
                                }
                            }
                            if (hasFailures(content)) {
                                throw new Exception("bulk error" + content);
                            }
                            long oneBatchEndTime = System.currentTimeMillis();
                            if (oneBatchEndTime - oneBatchStartTime > 5000L)
                                LOGGER.info("one batch cost:" + (oneBatchEndTime - oneBatchStartTime));
                        }
                        return null;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

    }

    @Override
    public void close() throws Exception {
        esClient.close();
    }

    private boolean hasFailures(String content) {
        JSONObject restJson = JSON.parseObject(content);
        if (!restJson.getBoolean("errors")) {
            return false;
        }
        JSONArray items = restJson.getJSONArray("items");
        for (int i = 0; i < items.size(); i++) {
            JSONObject item = (JSONObject) items.getJSONObject(i).entrySet().iterator().next().getValue();
            if (item.containsKey("error") &&
                    item.getJSONObject("error").getString("type").equals("version_conflict_engine_exception")) {
                continue;
            } else {
                if (item.containsKey("error")) {
                    return true;
                }
            }
        }
        return false;
    }
}
