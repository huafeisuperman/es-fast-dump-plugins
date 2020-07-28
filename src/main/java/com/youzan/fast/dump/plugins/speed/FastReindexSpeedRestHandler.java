package com.youzan.fast.dump.plugins.speed;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.RestBuilderListener;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.07.24
 */
public class FastReindexSpeedRestHandler extends BaseRestHandler {

    public FastReindexSpeedRestHandler(RestController restController) {
        restController.registerHandler(RestRequest.Method.PUT, "/fast/index/speed/{id}", this);
    }

    @Override
    public String getName() {
        return "fast_reindex_speed";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        FastReindexSpeedRequest reindexSpeedRequest = new FastReindexSpeedRequest();
        reindexSpeedRequest.setId(request.param("id"));
        reindexSpeedRequest.setSpeed(request.paramAsInt("speed", 0));

        return channel -> client.executeLocally(FastReindexSpeedAction.INSTANCE, reindexSpeedRequest, new RestBuilderListener<FastReindexSpeedResponse>(channel) {
            @Override
            public RestResponse buildResponse(FastReindexSpeedResponse fastReindexSpeedResponse, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(RestStatus.OK, fastReindexSpeedResponse.toXContent(builder, ToXContent.EMPTY_PARAMS));
            }
        });
    }
}
