package com.youzan.fast.dump.resolver;

import com.youzan.fast.dump.plugins.FastReindexShardRequest;

/**
 * @author :  43333
 * @Project Name :  mainline
 * @Package Name :  com.youzan.clouddb.tools.general.resolver
 * @Description :  TODO
 * @Creation Date:  2018-08-16 9:30
 * --------  ---------  --------------------------
 */
public class DataResolveFactory {

    public static DataResolve getDataResolve(FastReindexShardRequest request) throws Exception {
        switch (request.getFastReindexRequest().getTargetResolver().toUpperCase()) {
            case "ES":
                return new ESDataResolve(request.getFastReindexRequest().getRemoteInfo().getIp(), request.getFastReindexRequest().getRemoteInfo().getPort(),
                        request.getFastReindexRequest().getRemoteInfo().getClusterName(), request.getFastReindexRequest().getMode(), request.getFastReindexRequest().getPerNodeSpeedLimit());
            default:
                throw new Exception("not find dataResolve");
        }
    }
}
