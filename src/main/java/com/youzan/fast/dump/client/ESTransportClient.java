package com.youzan.fast.dump.client;

import com.youzan.fast.dump.plugins.FastReindexRequest;
import org.elasticsearch.client.Client;

/**
 * @author :  43333
 * @Project Name :  mainline
 * @Package Name :  com.youzan.clouddb.tools.general.client
 * @Description :  TODO
 * @Creation Date:  2018-08-28 10:29
 * --------  ---------  --------------------------
 */
public class ESTransportClient extends AbstractEsClient<Client> {

    public ESTransportClient(Client client) {
        super(null);
        esClient =  client;
    }

    @Override
    public Client buildClient(FastReindexRequest.FastReindexRemoteInfo remoteInfo) throws Exception {
        return esClient;
    }
}
