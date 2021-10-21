package com.youzan.fast.dump.resource;

import com.google.common.collect.ArrayListMultimap;
import com.youzan.fast.dump.plugins.FastReindexRequest;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2019.11.19
 */
public interface FileResource {

    /**
     * 获取资源文件，以及每个节点的分配
     *
     * @param resources
     * @param sourceInfo
     * @param indexType
     * @param targetResource
     * @return
     * @throws Exception
     */
    ArrayListMultimap getSlaveFile(String[] resources, FastReindexRequest.FastReindexRemoteInfo sourceInfo,
                                   String indexType, String targetResource, String type) throws Exception;
}
