package com.youzan.fast.dump.client;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * @author :  43333
 * @Project Name :  mainline
 * @Package Name :  com.youzan.clouddb.tools.general.client
 * @Description :  TODO
 * @Creation Date:  2018-08-28 10:29
 * --------  ---------  --------------------------
 */
public class ESClient implements Client<TransportClient> {

    private TransportClient esClient;

    private String ip;

    private int port;

    private String esName;

    public ESClient(String ip, String port, String esName) {
        this.ip = ip;
        this.port = Integer.parseInt(port);
        this.esName = esName;
    }

    @Override
    public TransportClient getClient() throws Exception {
        if (esClient == null) {
            Settings settings = Settings.builder().put("cluster.name", esName)
                    .put("client.transport.sniff", true)
                    .build();
            esClient = new PreBuiltTransportClient(settings);
            //esClient.addTransportAddress(new TransportAddress(InetAddress.getByName(ip), port));
            esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), port));
        }
        return esClient;
    }
}
