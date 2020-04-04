package com.youzan.fast.dump.plugins;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.03.25
 */
public class FastReindexRequestBuilder extends ActionRequestBuilder<FastReindexRequest, FastReindexResponse, FastReindexRequestBuilder> {
    public FastReindexRequestBuilder(ElasticsearchClient client, Action<FastReindexRequest, FastReindexResponse, FastReindexRequestBuilder> action) {
        super(client, action, new FastReindexRequest());
    }


}
