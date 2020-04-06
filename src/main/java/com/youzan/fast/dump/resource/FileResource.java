package com.youzan.fast.dump.resource;

import com.google.common.collect.ArrayListMultimap;
import org.elasticsearch.client.Client;

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
     * @param resource
     * @param client
     * @param targetIndexType
     * @param targetIndex
     * @return
     * @throws Exception
     */
    ArrayListMultimap getSlaveFile(String[] resource, Client client,
                                   String targetIndexType, String targetIndex) throws Exception;

}
