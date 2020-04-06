package com.youzan.fast.dump.plugins;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.youzan.fast.dump.common.IndexModeEnum;
import com.youzan.fast.dump.common.IndexTypeEnum;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
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

    public FastReindexRestHandler(Settings settings, RestController restController) {
        super(settings);
        restController.registerHandler(RestRequest.Method.POST, "/fast/index", this);
        restController.registerHandler(RestRequest.Method.GET, "/fast/processInfo", this);
    }


    private static final String SOURCE_INDEX = "source_index";
    private static final String MODE = "mode";
    private static final String TARGET_INDEX = "target_index";
    private static final String TARGET_IP = "target_ip";
    private static final String TARGET_PORT = "target_port";
    private static final String TARGET_NAME = "target_name";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String TARGET_RESOLVER = "target_resolver";
    private static final String BATCH_SIZE = "batch_size";
    private static final String THREAD_NUM = "thread_num";
    private static final String ONE_FILE_THREAD_NUM = "one_file_thread_num";
    private static final String TARGET_INDEX_TYPE = "target_index_type";
    private static final String PACKAGE_NAME = "com.youzan.fast.dump.common.rules.";


    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        FastReindexRequest fastReindexRequest = new FastReindexRequest();

        JSONObject param = JSON.parseObject(request.content().utf8ToString());

        JSONObject source = param.getJSONObject("source");
        JSONObject target = param.getJSONObject("target");
        JSONObject remoteInfo = target.getJSONObject("remote_info");
        JSONObject rules = param.getJSONObject("rules");

        fastReindexRequest.setSourceIndex(source.getString(SOURCE_INDEX));
        fastReindexRequest.setMode(source.getOrDefault(MODE, IndexModeEnum.UPDATE.getMode()).toString());
        fastReindexRequest.setBatchSize(Integer.parseInt(source.getOrDefault(BATCH_SIZE, "1000").toString()));
        fastReindexRequest.setThreadNum(Integer.parseInt(source.getOrDefault(THREAD_NUM, "1").toString()));
        fastReindexRequest.setOneFileThreadNum(Integer.parseInt(source.getOrDefault(ONE_FILE_THREAD_NUM, "1").toString()));
        AccessController.doPrivileged(
                (PrivilegedAction<Void>) () -> {
                    fastReindexRequest.setQuery(source.getString("query"));
                    fastReindexRequest.checkQuery();
                    return null;
                });

        fastReindexRequest.setTargetIndex(target.getString(TARGET_INDEX));
        fastReindexRequest.setTargetIndexType(target.getOrDefault(TARGET_INDEX_TYPE, IndexTypeEnum.ALL_TO_ALL.getIndexType()).toString());
        fastReindexRequest.setTargetResolver(target.getString(TARGET_RESOLVER));

        if (null != rules) {
            FastReindexRequest.RuleInfo ruleInfo = new FastReindexRequest.RuleInfo();
            ruleInfo.setRuleName(PACKAGE_NAME + rules.getString("rule_name"));
            ruleInfo.setField(rules.getString("field"));
            ruleInfo.setRules(rules.getString("rule_value"));
        }


        if (null != remoteInfo) {
            FastReindexRequest.FastReindexRemoteInfo fastReindexRemoteInfo = new FastReindexRequest.FastReindexRemoteInfo();
            fastReindexRemoteInfo.setIp(remoteInfo.getString(TARGET_IP));
            fastReindexRemoteInfo.setPort(remoteInfo.getInteger(TARGET_PORT));
            fastReindexRemoteInfo.setClusterName(remoteInfo.getString(TARGET_NAME));
            fastReindexRemoteInfo.setUsername(remoteInfo.getString(USERNAME));
            fastReindexRemoteInfo.setPassword(remoteInfo.getString(PASSWORD));
            if (remoteInfo.containsKey("headers")) {
                fastReindexRemoteInfo.setHeaders(remoteInfo.getJSONObject("headers").getInnerMap());
            }

            fastReindexRequest.setRemoteInfo(fastReindexRemoteInfo);
        }


        Boolean waitForCompletion = request.paramAsBoolean("wait_for_completion", true);
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