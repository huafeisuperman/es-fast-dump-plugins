package com.youzan.fast.dump.resource;

import com.google.common.collect.ArrayListMultimap;

import java.util.List;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2019.12.10
 */
public abstract class AbstractFileResource implements FileResource {
    private List<String> ips;

    public AbstractFileResource(List ips) {
        this.ips = ips;
    }

    public void assignResourceToNode(String targetIndex, ArrayListMultimap resultMap, String type, List<String> fileList) throws Exception {
        for (int i = 0; i < fileList.size(); i++) {
            resultMap.put(ips.get(i % ips.size()), targetIndex + ":" + type + ":" + fileList.get(i));
        }
    }
}
