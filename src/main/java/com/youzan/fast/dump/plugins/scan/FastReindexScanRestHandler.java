package com.youzan.fast.dump.plugins.scan;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.RestBuilderListener;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.08.07
 */
public class FastReindexScanRestHandler extends BaseRestHandler {

    public FastReindexScanRestHandler(RestController restController) {
        restController.registerHandler(RestRequest.Method.GET, "/fast/index/scan/{id}", this);
    }

    @Override
    public String getName() {
        return "fast_reindex_scan";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        FastReindexScanRequest reindexScanRequest = new FastReindexScanRequest();
        reindexScanRequest.setId(request.param("id"));
        return channel -> client.execute(FastReindexScanAction.INSTANCE, reindexScanRequest, new RestBuilderListener<FastReindexScanResponse>(channel) {
            @Override
            public RestResponse buildResponse(FastReindexScanResponse fastReindexScanResponse, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(RestStatus.OK, fastReindexScanResponse.toXContent(builder, ToXContent.EMPTY_PARAMS));
            }
        });
    }
}
