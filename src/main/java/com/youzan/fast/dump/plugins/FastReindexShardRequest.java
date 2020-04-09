package com.youzan.fast.dump.plugins;

import lombok.Data;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.03.27
 */
@Data
public class FastReindexShardRequest extends ActionRequest {

    private String nodeId;

    private List<String> file;

    private FastReindexRequest fastReindexRequest;

    public FastReindexShardRequest() {
        super();
    }

    public Task createTask(long id, String type, String action, TaskId parentTaskId) {
        return fastReindexRequest.createTask(id, type, action, parentTaskId);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        fastReindexRequest = new FastReindexRequest();
        nodeId = in.readString();
        file = Arrays.asList(in.readStringArray());
        fastReindexRequest.setMode(in.readString());
        fastReindexRequest.setSourceIndex(in.readString());
        fastReindexRequest.setTargetIndex(in.readString());
        fastReindexRequest.setTargetResolver(in.readString());
        fastReindexRequest.setTargetIndexType(in.readString());
        fastReindexRequest.setTargetType(in.readOptionalString());
        fastReindexRequest.setBatchSize(in.readInt());
        fastReindexRequest.setOneFileThreadNum(in.readInt());
        fastReindexRequest.setPerNodeSpeedLimit(in.readInt());
        fastReindexRequest.setThreadNum(in.readInt());
        fastReindexRequest.setQuery(in.readOptionalString());
        fastReindexRequest.setRemoteInfo(in.readOptionalWriteable(FastReindexRequest.FastReindexRemoteInfo::new));
        fastReindexRequest.setRuleInfo(in.readOptionalWriteable(FastReindexRequest.RuleInfo::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeId);
        out.writeStringArray(file.toArray(new String[]{}));
        out.writeString(fastReindexRequest.getMode());
        out.writeString(fastReindexRequest.getSourceIndex());
        out.writeString(fastReindexRequest.getTargetIndex());
        out.writeString(fastReindexRequest.getTargetResolver());
        out.writeString(fastReindexRequest.getTargetIndexType());
        out.writeOptionalString(fastReindexRequest.getTargetType());
        out.writeInt(fastReindexRequest.getBatchSize());
        out.writeInt(fastReindexRequest.getOneFileThreadNum());
        out.writeInt(fastReindexRequest.getPerNodeSpeedLimit());
        out.writeInt(fastReindexRequest.getThreadNum());
        out.writeOptionalString(fastReindexRequest.getQuery());
        out.writeOptionalWriteable(fastReindexRequest.getRemoteInfo());
        out.writeOptionalWriteable(fastReindexRequest.getRuleInfo());
    }
}
