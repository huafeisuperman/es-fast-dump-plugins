package com.youzan.fast.dump.resolver;

import com.youzan.fast.dump.plugins.FastReindexShardRequest;
import org.elasticsearch.client.Client;

/**
 * @author :  43333
 * @Project Name :  mainline
 * @Package Name :  com.youzan.clouddb.tools.general.resolver
 * @Description :  TODO
 * @Creation Date:  2018-08-16 9:30
 * --------  ---------  --------------------------
 */
public class DataResolveFactory {

    public static DataResolve getDataResolve(FastReindexShardRequest request, Client client) throws Exception {
        switch (request.getFastReindexRequest().getTargetResolver().toUpperCase()) {
            case "ES":
                if (null == request.getFastReindexRequest().getRemoteInfo())
                    return new ESDataResolve(client,
                            request.getFastReindexRequest().getMode(), request.getFastReindexRequest().getPerNodeSpeedLimit());
                else
                    return new ESRestDataResolver(request.getFastReindexRequest().getRemoteInfo(),
                            request.getFastReindexRequest().getMode(), request.getFastReindexRequest().getPerNodeSpeedLimit());
            default:
                throw new Exception("not find dataResolve");
        }
    }
}
