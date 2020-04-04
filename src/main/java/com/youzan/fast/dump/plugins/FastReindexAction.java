package com.youzan.fast.dump.plugins;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.03.25
 */
public class FastReindexAction extends Action<FastReindexRequest, FastReindexResponse, FastReindexRequestBuilder> {
    public static final FastReindexAction INSTANCE = new FastReindexAction();
    public static final String NAME = "indices:fast/index";

    public FastReindexAction() {
        super(NAME);
    }

    @Override
    public FastReindexRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new FastReindexRequestBuilder(client, this);
    }

    @Override
    public FastReindexResponse newResponse() {
        return new FastReindexResponse();
    }
}
