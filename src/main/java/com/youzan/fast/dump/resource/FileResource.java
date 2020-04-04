package com.youzan.fast.dump.resource;

import com.google.common.collect.ArrayListMultimap;
import com.youzan.fast.dump.client.Client;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2019.11.19
 */
public interface FileResource {

    /**
     * 获取资源文件，以及每个节点的分配
     * @param resource
     * @param sourceIp
     * @param sourcePort
     * @param sourceName
     * @param targetIndexType
     * @param targetIndex
     * @return
     * @throws Exception
     */
    ArrayListMultimap getSlaveFile(String[] resource, String sourceIp,
                                   String sourcePort, String sourceName,
                                   String targetIndexType, String targetIndex) throws Exception;

}
