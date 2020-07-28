package com.youzan.fast.dump.plugins;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.youzan.fast.dump.common.IndexModeEnum;
import com.youzan.fast.dump.common.IndexTypeEnum;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.tasks.LoggingTaskListener;
import org.elasticsearch.tasks.Task;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.03.25
 */
public class FastReindexRestHandler extends BaseRestHandler {

    public FastReindexRestHandler(RestController restController) {
        restController.registerHandler(RestRequest.Method.POST, "/fast/index", this);
    }


    private static final String SOURCE = "source";
    private static final String TARGET = "target";
    private static final String REMOTE_INFO = "remote_info";
    private static final String RULES = "rules";
    private static final String SOURCE_INDEX = "source_index";
    private static final String MODE = "mode";
    private static final String TARGET_INDEX = "target_index";
    private static final String IP = "ip";
    private static final String PORT = "port";
    private static final String CLUSTER_NAME = "cluster_name";
    private static final String TARGET_TYPE = "target_type";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String TARGET_RESOLVER = "target_resolver";
    private static final String BATCH_SIZE = "batch_size";
    private static final String THREAD_NUM = "thread_num";
    private static final String SPEED_LIMIT = "speed_limit";
    private static final String ONE_FILE_THREAD_NUM = "one_file_thread_num";
    private static final String TARGET_INDEX_TYPE = "target_index_type";
    private static final String PACKAGE_NAME = "com.youzan.fast.dump.common.rules.";
    private static final String RULE_NAME = "rule_name";
    private static final String FIELD = "field";
    private static final String RULE_VALUE = "rule_value";
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final int DEFAULT_THREAD_NUM = 1;
    private static final int DEFAULT_SPEED_LIMIT = 100000;
    private static final int DEFAULT_ONE_FILE_THREAD_NUM = 1;
    private static final String HEADERS = "headers";
    private static final String WAIT_FOR_COMPLETION = "wait_for_completion";
    private static final String SOURCE_RESOLVER = "source_resolver";
    private static final String NEED_FIELDS = "need_fields";
    private static final String NEST_FIELDS = "nest_fields";
    private static final String PRIMARY_KEY = "primary_key";
    private static final String SOURCE_INFO = "source_info";


    @Override
    public String getName() {
        return "fast_index";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        FastReindexRequest fastReindexRequest = new FastReindexRequest();

        JSONObject param = JSON.parseObject(request.content().utf8ToString());

        JSONObject source = param.getJSONObject(SOURCE);
        JSONObject sourceInfo = source.getJSONObject(SOURCE_INFO);
        JSONObject target = param.getJSONObject(TARGET);
        JSONObject remoteInfo = target.getJSONObject(REMOTE_INFO);
        JSONObject rules = param.getJSONObject(RULES);

        fastReindexRequest.setSourceIndex(source.getString(SOURCE_INDEX));
        fastReindexRequest.setMode(source.getOrDefault(MODE, IndexModeEnum.UPDATE.getMode()).toString());
        fastReindexRequest.setBatchSize(Integer.parseInt(source.getOrDefault(BATCH_SIZE, DEFAULT_BATCH_SIZE).toString()));
        fastReindexRequest.setThreadNum(Integer.parseInt(source.getOrDefault(THREAD_NUM, DEFAULT_THREAD_NUM).toString()));
        fastReindexRequest.setSpeedLimit(Integer.parseInt(source.getOrDefault(SPEED_LIMIT, DEFAULT_SPEED_LIMIT).toString()));
        fastReindexRequest.setOneFileThreadNum(Integer.parseInt(source.getOrDefault(ONE_FILE_THREAD_NUM, DEFAULT_ONE_FILE_THREAD_NUM).toString()));
        AccessController.doPrivileged(
                (PrivilegedAction<Void>) () -> {
                    fastReindexRequest.setQuery(source.getString("query"));
                    if (null != fastReindexRequest.getQuery()) {
                        fastReindexRequest.checkQuery();
                    }
                    return null;
                });

        fastReindexRequest.setTargetIndex(target.getString(TARGET_INDEX));
        fastReindexRequest.setTargetIndexType(target.getOrDefault(TARGET_INDEX_TYPE, IndexTypeEnum.ALL_TO_ALL.getIndexType()).toString());
        fastReindexRequest.setTargetResolver(target.getString(TARGET_RESOLVER));
        fastReindexRequest.setTargetType(target.getString(TARGET_TYPE));

        fastReindexRequest.setNestFields(source.getString(NEST_FIELDS));
        fastReindexRequest.setNeedFields(source.getString(NEED_FIELDS));
        fastReindexRequest.setPrimaryKey(source.getString(PRIMARY_KEY));
        fastReindexRequest.setSourceResolver(source.getString(SOURCE_RESOLVER));

        if (null != rules) {
            FastReindexRequest.RuleInfo ruleInfo = new FastReindexRequest.RuleInfo();
            ruleInfo.setRuleName(PACKAGE_NAME + rules.getString(RULE_NAME));
            ruleInfo.setField(rules.getString(FIELD));
            ruleInfo.setRules(rules.getString(RULE_VALUE));
            fastReindexRequest.setRuleInfo(ruleInfo);
        }

        if (null != sourceInfo) {
            fastReindexRequest.setSourceInfo(getRemoteInfo(sourceInfo));
        }

        if (null != remoteInfo) {
            fastReindexRequest.setRemoteInfo(getRemoteInfo(remoteInfo));
        }


        Boolean waitForCompletion = request.paramAsBoolean(WAIT_FOR_COMPLETION, true);
        fastReindexRequest.setShouldStoreResult(!waitForCompletion);
        logger.info("fast reindex param:[{}]", fastReindexRequest.toString());

        if (waitForCompletion) {
            // 跳转到transport.doExecute()
            return channel -> client.executeLocally(FastReindexAction.INSTANCE, fastReindexRequest, new RestBuilderListener<FastReindexResponse>(channel) {
                @Override
                public RestResponse buildResponse(FastReindexResponse fastReindexResponse, XContentBuilder builder) throws Exception {
                    return new BytesRestResponse(RestStatus.OK, fastReindexResponse.toXContent(builder));
                }
            });
        } else {
            return sendTask(client.getLocalNodeId(), client.executeLocally(FastReindexAction.INSTANCE, fastReindexRequest, LoggingTaskListener.instance()));
        }


    }

    private FastReindexRequest.FastReindexRemoteInfo getRemoteInfo(JSONObject info) {
        FastReindexRequest.FastReindexRemoteInfo fastReindexRemoteInfo = new FastReindexRequest.FastReindexRemoteInfo();
        fastReindexRemoteInfo.setIp(info.getString(IP));
        fastReindexRemoteInfo.setPort(info.getInteger(PORT));
        fastReindexRemoteInfo.setClusterName(info.getString(CLUSTER_NAME));
        fastReindexRemoteInfo.setUsername(info.getString(USERNAME));
        fastReindexRemoteInfo.setPassword(info.getString(PASSWORD));
        if (info.containsKey(HEADERS)) {
            fastReindexRemoteInfo.setHeaders(info.getJSONObject(HEADERS).getInnerMap());
        }
        return fastReindexRemoteInfo;
    }


    private RestChannelConsumer sendTask(String localNodeId, Task task) {
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("task", localNodeId + ":" + task.getId());
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        };
    }

}