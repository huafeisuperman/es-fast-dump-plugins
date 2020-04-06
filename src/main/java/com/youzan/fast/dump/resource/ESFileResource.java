package com.youzan.fast.dump.resource;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.ArrayListMultimap;
import com.youzan.fast.dump.client.ESTransportClient;
import com.youzan.fast.dump.common.IndexTypeEnum;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsNodeResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.ShardRouting;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2019.11.20
 */
public class ESFileResource implements FileResource {

    /**
     * 采用主副对应寻找的方式，尽可能让每个节点分配的资源数一样
     *
     * @param indices      索引数组
     * @param sourceEsIp   es ip
     * @param sourceEsPort es port
     * @param sourceEsName es name
     * @return 节点对应的资源数
     * @throws Exception 获取资源数失败
     */
    @Override
    public ArrayListMultimap getSlaveFile(String indices[], Client client, String targetIndexType, String targetIndex) throws Exception {
        ESTransportClient esClient = new ESTransportClient(client);
        List<ClusterStatsNodeResponse> response = esClient.getClient().
                admin().cluster().prepareClusterStats().get().getNodes();
        Set<String> idSet = new HashSet<>();
        Map<String, Queue<String>> multimap = new LinkedHashMap<>();

        //获取索引和type
        Map<String, String> indexAndType = getIndexAndType(indices, esClient);

        //获取索引映射信息
        Map<String, String> indexMapping = getIndexMapping(esClient, targetIndexType, targetIndex, indexAndType);

        ArrayListMultimap resultMap = ArrayListMultimap.create();
        int primaryShard = 0;

        //获取索引的shard
        for (ClusterStatsNodeResponse stat : response) {
            for (ShardStats shardStats : stat.shardsStats()) {
                ShardRouting shardRouting = shardStats.getShardRouting();
                if (indexAndType.containsKey(shardRouting.getIndexName())) {
                    primaryShard = shardRouting.primary() ? primaryShard + 1 : primaryShard;
                    String path = shardStats.getDataPath() + "/indices/" + shardRouting.index().getUUID() + "/" + shardRouting.getId() + "/index";
                    Queue queue = multimap.get(stat.getNode().getId());
                    if (null == queue) {
                        queue = new LinkedBlockingQueue();
                        multimap.put(stat.getNode().getId(), queue);
                    }

                    String tmpTargetIndex = indexMapping.containsKey(shardRouting.getIndexName()) ?
                            indexMapping.get(shardRouting.getIndexName()) : shardRouting.getIndexName();
                    queue.add(shardRouting.index().getUUID() + ":" +
                            shardRouting.getId() + "," + tmpTargetIndex + ":" +
                            indexAndType.get(shardRouting.getIndexName()) + ":" + path + ":" + shardRouting.getIndexName());
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
     * @param targetIndexType 索引类型
     * @param targetIndex     目标索引
     * @param indexAndType    index和type的对应关系
     * @return Map<String, String> key为source索引，value为目标索引
     * @throws Exception
     */
    private Map<String, String> getIndexMapping(ESTransportClient esClient, String targetIndexType,
                                                String targetIndex, Map<String, String> indexAndType) throws Exception {
        Map<String, String> indexMapping = new HashMap<>();
        switch (IndexTypeEnum.findIndexTypeEnum(targetIndexType)) {
            case ONE_TO_ONE:
                String[] sourceToTargets = targetIndex.split(",");
                for (String sourceToTarget : sourceToTargets) {
                    String[] indexArray = sourceToTarget.split("->");
                    indexMapping.put(getTrueIndex(esClient, indexArray[0]),
                            getTrueIndex(esClient, indexArray[1]));
                }
                break;
            case ALL_TO_ALL:
            case CUSTOM:
                indexAndType.keySet().forEach(x -> indexMapping.put(x, x));
                break;
            case ALL_TO_ONE:
                String trueIndex = getTrueIndex(esClient, targetIndex);
                indexAndType.keySet().forEach(x -> indexMapping.put(x, trueIndex));
                break;
            default:
                throw new Exception("未知类型:" + targetIndexType);
        }
        return indexMapping;
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

    /**
     * 根据别名获取index和type
     *
     * @param indices  索引数组
     * @param esClient client
     * @return Map<String, String>,key位index,value为type
     * @throws Exception 获取索引失败
     */
    private Map<String, String> getIndexAndType(String[] indices, ESTransportClient esClient) throws Exception {
        Map<String, String> indexAndType = new HashMap<>();
        Set<String> indexSet = new HashSet<>();
        for (String index : indices) {
            indexSet.add(getTrueIndex(esClient, index));
        }

        //索引刷新
        FlushResponse flushResponse = esClient.getClient().admin().indices().
                prepareFlush(indexSet.toArray(new String[]{})).setForce(true).get();

        if (0 < flushResponse.getFailedShards()) {
            throw new Exception("flush error, failed shard number:" + flushResponse.getFailedShards());
        }

        //获取mapping
        GetMappingsResponse mappingsResponse = esClient.getClient().admin().indices().
                getMappings(new GetMappingsRequest().indices(indexSet.toArray(new String[]{}))).get();

        for (ObjectCursor<String> key : mappingsResponse.getMappings().keys()) {
            indexAndType.put(key.value, mappingsResponse.getMappings().get(key.value).keys().toArray()[0].toString());
        }
        return indexAndType;
    }

    private String getTrueIndex(ESTransportClient esClient, String index) throws Exception {
        String[] trueIndices = esClient.getClient().admin().indices().
                getAliases(new GetAliasesRequest(index)).
                get().getAliases().keys().toArray(String.class);
        //如果是别名则加上别名对应索引，否则加上真实索引
        //TODO 一个别名对应多个索引的情况暂时先不支持,只会返回第一个索引
        if (trueIndices.length > 0) {
            return trueIndices[0];
        } else {
            return index;
        }
    }
}
