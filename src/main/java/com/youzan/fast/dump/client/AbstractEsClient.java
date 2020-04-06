package com.youzan.fast.dump.client;

import com.youzan.fast.dump.plugins.FastReindexRequest;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.04.04
 */
public abstract class AbstractEsClient<T> implements Client<T> {

    protected T esClient;

    protected FastReindexRequest.FastReindexRemoteInfo remoteInfo;

    public AbstractEsClient(FastReindexRequest.FastReindexRemoteInfo remoteInfo) {
        this.remoteInfo = remoteInfo;
    }

    @Override
    public T getClient() throws Exception {
        if (esClient == null) {
            esClient = buildClient(remoteInfo);
        }
        return esClient;
    }

    public abstract T buildClient(FastReindexRequest.FastReindexRemoteInfo remoteInfo) throws Exception;
}
