package com.youzan.fast.dump.resource;

import com.google.common.collect.ArrayListMultimap;
import com.youzan.fast.dump.client.ESTransportClient;
import com.youzan.fast.dump.common.IndexTypeEnum;
import com.youzan.fast.dump.plugins.FastReindexRequest;
import lombok.Data;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsNodeResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.ShardRouting;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2019.11.20
 */
public class ESFileResource implements FileResource {

    private Client client;

    public ESFileResource(Client client) {
        this.client = client;
    }

    /**
     * 采用主副对应寻找的方式，尽可能让每个节点分配的资源数一样
     *
     * @param resources
     * @param sourceInfo
     * @param indexType
     * @param targetResource
     * @return
     * @throws Exception
     */
    @Override
    public ArrayListMultimap getSlaveFile(String[] resources, FastReindexRequest.FastReindexRemoteInfo sourceInfo, String indexType, String targetResource, String type) throws Exception {
        ESTransportClient esClient = new ESTransportClient(client);
        List<ClusterStatsNodeResponse> response = esClient.getClient().
                admin().cluster().prepareClusterStats().get().getNodes();
        Set<String> idSet = new HashSet<>();
        Map<String, Queue<String>> multimap = new LinkedHashMap<>();

        //获取索引
        flushIndex(resources, esClient);

        //获取索引映射信息
        Map<String, String> indexRelation = getIndexRelation(esClient, indexType, targetResource, resources);

        ArrayListMultimap resultMap = ArrayListMultimap.create();
        int primaryShard = 0;

        //获取索引的shard
        for (ClusterStatsNodeResponse stat : response) {
            for (ShardStats shardStats : stat.shardsStats()) {
                ShardRouting shardRouting = shardStats.getShardRouting();
                if (indexRelation.containsKey(shardRouting.getIndexName())) {
                    primaryShard = shardRouting.primary() ? primaryShard + 1 : primaryShard;
                    String path = shardStats.getDataPath() + "/indices/" + shardRouting.index().getUUID() + "/" + shardRouting.getId() + "/index";
                    Queue queue = multimap.get(stat.getNode().getId());
                    if (null == queue) {
                        queue = new LinkedBlockingQueue();
                        multimap.put(stat.getNode().getId(), queue);
                    }

                    if (null == indexRelation.get(shardRouting.getIndexName())) {
                        throw new RuntimeException("index not found or source -> target not match");
                    }

                    queue.add(shardRouting.index().getUUID() + ":" +
                            shardRouting.getId() + "," + indexRelation.get(shardRouting.getIndexName()) + ":" +
                            (type == null ? "_doc" : type) + ":" + path + ":" + shardRouting.getIndexName());
                }
            }
        }

        //轮训获取对应节点的ip
        String nextIp = null;
        while (idSet.size() < primaryShard) {
            for (Map.Entry<String, Queue<String>> entry : multimap.entrySet()) {
                if (null != nextIp && !entry.getKey().equals(nextIp)) {
                    continue;
                }
                String idAndPath;
                while (null != (idAndPath = entry.getValue().poll())) {
                    String[] value = idAndPath.split(",");
                    if (idSet.add(value[0])) {
                        resultMap.put(entry.getKey(), value[1]);
                        nextIp = getNextIp(value[0], multimap.entrySet());
                        break;
                    } else {
                        nextIp = null;
                    }
                }
            }
        }
        return resultMap;
    }

    /**
     * 获取源索引和目标索引的对应关系
     *
     * @param esClient
     * @param targetIndexType
     * @param targetResource
     * @param sourceResource
     * @return
     * @throws Exception
     */
    private Map<String, String> getIndexRelation(ESTransportClient esClient, String targetIndexType,
                                                 String targetResource, String[] sourceResource) throws Exception {
        Map<String, String> indexRelation = new HashMap<>();
        switch (IndexTypeEnum.findIndexTypeEnum(targetIndexType)) {
            case ONE_TO_ONE:
                String[] sourceToTargets = targetResource.split(",");
                for (String sourceToTarget : sourceToTargets) {
                    String[] indexArray = sourceToTarget.split("->");
                    putTrueIndex(esClient, indexArray[0], indexRelation, indexArray[1]);
                }
                break;
            case ALL_TO_ALL:
                Stream.of(sourceResource).forEach(x -> putTrueIndex(esClient, x, indexRelation, null));
                break;
            case CUSTOM:
            case ALL_TO_ONE:
                Stream.of(sourceResource).forEach(x -> putTrueIndex(esClient, x, indexRelation, targetResource));
                break;
            default:
                throw new Exception("未知类型:" + targetIndexType);
        }
        return indexRelation;
    }

    private String getNextIp(String id, Set<Map.Entry<String, Queue<String>>> entrySet) {
        for (Map.Entry<String, Queue<String>> entry : entrySet) {
            for (String s : entry.getValue()) {
                if (s.split(",")[0].equals(id)) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }


    private void flushIndex(String[] indices, ESTransportClient esClient) throws Exception {
        //索引刷新
        FlushResponse flushResponse = esClient.getClient().admin().indices().
                prepareFlush(indices).setForce(true).get();

        if (0 < flushResponse.getFailedShards()) {
            throw new Exception("flush error, failed shard number:" + flushResponse.getFailedShards());
        }
    }

    private void putTrueIndex(ESTransportClient esClient, String index, Map<String, String> indexRelation, String targetResource) {
        try {
            String[] trueIndices = esClient.getClient().admin().indices().
                    getAliases(new GetAliasesRequest(index)).
                    get().getAliases().keys().toArray(String.class);

            if (trueIndices.length == 0) {
                trueIndices = new String[]{index};
            }


            if (targetResource == null) {
                Stream.of(trueIndices).forEach(x -> indexRelation.put(x, x));
            } else {
                Stream.of(trueIndices).forEach(x -> indexRelation.put(x, targetResource));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Data
    class IndexAliases {
        private String indexName;
        private String aliases;
        private String uuid;
    }
}
