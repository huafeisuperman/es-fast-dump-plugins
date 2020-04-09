package com.youzan.fast.dump.client;

import com.youzan.fast.dump.plugins.FastReindexRequest;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author :  43333
 * @Project Name :  mainline
 * @Package Name :  com.youzan.clouddb.tools.general.client
 * @Description :  TODO
 * @Creation Date:  2018-08-28 10:29
 * --------  ---------  --------------------------
 */
public class ESRestClient extends AbstractEsClient<RestClient> {


    public ESRestClient(FastReindexRequest.FastReindexRemoteInfo remoteInfo) {
        super(remoteInfo);
    }


    @Override
    public RestClient buildClient(FastReindexRequest.FastReindexRemoteInfo remoteInfo) {
        return buildRestClient(remoteInfo);
    }

    public RestClient buildRestClient(FastReindexRequest.FastReindexRemoteInfo remoteInfo) {
        int i = 0;
        Header[] clientHeaders = new Header[remoteInfo.getHeaders().size()];
        for (Map.Entry<String, Object> header : remoteInfo.getHeaders().entrySet()) {
            clientHeaders[i] = new BasicHeader(header.getKey(), header.getValue().toString());
            i++;
        }
        String[] hosts = remoteInfo.getIp().split(",");
        List<HttpHost> httpHosts = new ArrayList<>();
        for (String host : hosts) {
            httpHosts.add(new HttpHost(host, remoteInfo.getPort(), null));
        }

        return RestClient.builder(httpHosts.toArray(new HttpHost[]{}))
                .setDefaultHeaders(clientHeaders)
                .setRequestConfigCallback(c -> {
                    c.setConnectTimeout(Math.toIntExact(remoteInfo.getConnectTimeout()));
                    c.setSocketTimeout(Math.toIntExact(remoteInfo.getSocketTimeout()));
                    return c;
                })
                .setHttpClientConfigCallback(c -> {
                    // Enable basic auth if it is configured
                    if (remoteInfo.getUsername() != null) {
                        UsernamePasswordCredentials creds = new UsernamePasswordCredentials(remoteInfo.getUsername(),
                                remoteInfo.getPassword());
                        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                        credentialsProvider.setCredentials(AuthScope.ANY, creds);
                        c.setDefaultCredentialsProvider(credentialsProvider);
                    }
                    // Limit ourselves to one reactor thread because for now the search process is single threaded.
                    //c.setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(10).build());
                    return c;
                }).setMaxRetryTimeoutMillis((int) remoteInfo.getSocketTimeout()).build();
    }
}
