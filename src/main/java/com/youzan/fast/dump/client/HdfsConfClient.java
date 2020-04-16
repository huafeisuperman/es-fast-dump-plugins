package com.youzan.fast.dump.client;

import com.youzan.fast.dump.plugins.FastReindexRequest;
import org.apache.hadoop.conf.Configuration;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.04.15
 */
public class HdfsConfClient extends AbstractEsClient<Configuration> {

    public HdfsConfClient(FastReindexRequest.FastReindexRemoteInfo remoteInfo) {
        super(remoteInfo);
    }

    @Override
    public Configuration buildClient(FastReindexRequest.FastReindexRemoteInfo remoteInfo) throws Exception {
        Configuration conf = AccessController.doPrivileged(
                (PrivilegedAction<Configuration>) () -> {
                    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
                    Configuration confTmp = new Configuration();
                    System.setProperty("HADOOP_USER_NAME", "search");
                    confTmp.set("fs.defaultFS", String.format("hdfs://%s", remoteInfo.getClusterName()));
                    confTmp.set("dfs.nameservices", remoteInfo.getClusterName());
                    confTmp.set(String.format("dfs.ha.namenodes.%s", remoteInfo.getClusterName()), "nn1,nn2");
                    confTmp.set(String.format("dfs.namenode.rpc-address.%s.nn1", remoteInfo.getClusterName()), remoteInfo.getIp().split(",")[0] + ":" + remoteInfo.getPort());
                    confTmp.set(String.format("dfs.namenode.rpc-address.%s.nn2", remoteInfo.getClusterName()), remoteInfo.getIp().split(",")[1] + ":" + remoteInfo.getPort());
                    confTmp.set(String.format("dfs.client.failover.proxy.provider.%s", remoteInfo.getClusterName()),
                            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
                    return confTmp;
                });

        return conf;

    }
}
