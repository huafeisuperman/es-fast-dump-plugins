package com.youzan.fast.dump.resource;

import com.google.common.collect.ArrayListMultimap;
import com.youzan.fast.dump.client.HdfsConfClient;
import com.youzan.fast.dump.plugins.FastReindexRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2019.11.19
 */
public class HiveFileResource extends AbstractFileResource implements FileResource {

    public HiveFileResource(List ips) {
        super(ips);
    }

    @Override
    public ArrayListMultimap getSlaveFile(String[] resources, FastReindexRequest.FastReindexRemoteInfo sourceInfo,
                                          String indexType, String targetResource, String type) throws Exception {
        ArrayListMultimap resultMap = ArrayListMultimap.create();
        Configuration conf = new HdfsConfClient(sourceInfo).getClient();
        AccessController.doPrivileged(
                (PrivilegedAction<Configuration>) () -> {
                    try {
                        FileSystem fs = FileSystem.get(conf);
                        List<String> fileList = new ArrayList<>();
                        for (String resource : resources) {
                            RemoteIterator<LocatedFileStatus> list = fs.listFiles(new Path(resource), false);
                            while (list.hasNext()) {
                                fileList.add(list.next().getPath().toString().split(sourceInfo.getClusterName())[1]);
                            }
                        }
                        assignResourceToNode(targetResource, resultMap, type, fileList);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        return resultMap;

    }
}
