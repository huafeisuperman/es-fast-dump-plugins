package com.youzan.fast.dump.plugins;

import lombok.Data;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.03.25
 */
@Data
public class FastReindexRequest extends ActionRequest {

    public FastReindexRequest() {

    }

    protected String sourceIndex;

    protected String mode;

    protected String targetIndexType;

    protected String targetIndex;

    protected FastReindexRemoteInfo remoteInfo;

    protected String targetResolver;

    protected int perNodeSpeedLimit = 50000;

    private int batchSize = 1000;

    private int threadNum = 1;

    private int oneFileThreadNum = 1;

    private boolean shouldStoreResult = false;

    public void setShouldStoreResult(boolean shouldStoreResult) {
        this.shouldStoreResult = shouldStoreResult;
    }

    @Override
    public boolean getShouldStoreResult() {
        return shouldStoreResult;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId) {
        return new FastReindexTask(id, type, action, getDescription(), parentTaskId);
    }

    @Data
    public static class FastReindexRemoteInfo implements Writeable {

        protected String ip;

        protected String clusterName;

        protected int port;

        protected Map<String, Object> headers = new HashMap<>();

        protected String username;

        protected String password;

        protected long connectTimeout = 30000;

        protected long socketTimeout = 30000;

        public FastReindexRemoteInfo() {

        }

        /**
         * Read from a stream.
         */
        public FastReindexRemoteInfo(StreamInput in) throws IOException {
            ip = in.readString();
            clusterName = in.readOptionalString();
            port = in.readInt();
            headers =  in.readMap();
            username = in.readOptionalString();
            password = in.readOptionalString();
            connectTimeout = in.readLong();
            socketTimeout = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(ip);
            out.writeOptionalString(clusterName);
            out.writeInt(port);
            out.writeMap(headers);
            out.writeOptionalString(username);
            out.writeOptionalString(password);
            out.writeLong(connectTimeout);
            out.writeLong(socketTimeout);
        }
    }

    @Override
    public String getDescription() {
        return sourceIndex + "->" + targetIndex;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
