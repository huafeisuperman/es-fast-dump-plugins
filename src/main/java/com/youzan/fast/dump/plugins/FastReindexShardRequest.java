package com.youzan.fast.dump.plugins;

import lombok.Data;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.03.27
 */
@Data
public class FastReindexShardRequest extends ActionRequest  implements IndicesRequest.Replaceable {

    private String nodeId;

    private List<String> file;

    private int totalNodeSize;

    private FastReindexRequest fastReindexRequest;

    public FastReindexShardRequest(StreamInput in) throws IOException {
        super(in);
        fastReindexRequest = new FastReindexRequest();
        nodeId = in.readString();
        file = Arrays.asList(in.readStringArray());
        totalNodeSize = in.readInt();
        fastReindexRequest.setMode(in.readString());
        fastReindexRequest.setShardOption(in.readString());
        fastReindexRequest.setShardNumber(in.readOptionalString());
        fastReindexRequest.setSourceIndex(in.readString());
        fastReindexRequest.setSourceResolver(in.readString());
        fastReindexRequest.setTargetIndex(in.readString());
        fastReindexRequest.setTargetResolver(in.readString());
        fastReindexRequest.setTargetIndexType(in.readString());
        fastReindexRequest.setTargetType(in.readOptionalString());
        fastReindexRequest.setBatchSize(in.readInt());
        fastReindexRequest.setOneFileThreadNum(in.readInt());
        fastReindexRequest.setPerNodeSpeedLimit(in.readInt());
        fastReindexRequest.setSpeedLimit(in.readInt());
        fastReindexRequest.setThreadNum(in.readInt());
        fastReindexRequest.setQuery(in.readOptionalString());
        fastReindexRequest.setNeedFields(in.readOptionalString());
        fastReindexRequest.setPrimaryKey(in.readOptionalString());
        fastReindexRequest.setNestFields(in.readOptionalString());
        fastReindexRequest.setSourceInfo(in.readOptionalWriteable(FastReindexRequest.FastReindexRemoteInfo::new));
        fastReindexRequest.setRemoteInfo(in.readOptionalWriteable(FastReindexRequest.FastReindexRemoteInfo::new));
        fastReindexRequest.setRuleInfo(in.readOptionalWriteable(FastReindexRequest.RuleInfo::new));
    }

    public FastReindexShardRequest() {

    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return fastReindexRequest.createTask(id, type, action, parentTaskId, headers);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeId);
        out.writeStringArray(file.toArray(new String[]{}));
        out.writeInt(totalNodeSize);
        out.writeString(fastReindexRequest.getMode());
        out.writeString(fastReindexRequest.getShardOption());
        out.writeOptionalString(fastReindexRequest.getShardNumber());
        out.writeString(fastReindexRequest.getSourceIndex());
        out.writeString(fastReindexRequest.getSourceResolver());
        out.writeString(fastReindexRequest.getTargetIndex());
        out.writeString(fastReindexRequest.getTargetResolver());
        out.writeString(fastReindexRequest.getTargetIndexType());
        out.writeOptionalString(fastReindexRequest.getTargetType());
        out.writeInt(fastReindexRequest.getBatchSize());
        out.writeInt(fastReindexRequest.getOneFileThreadNum());
        out.writeInt(fastReindexRequest.getPerNodeSpeedLimit());
        out.writeInt(fastReindexRequest.getSpeedLimit());
        out.writeInt(fastReindexRequest.getThreadNum());
        out.writeOptionalString(fastReindexRequest.getQuery());
        out.writeOptionalString(fastReindexRequest.getNeedFields());
        out.writeOptionalString(fastReindexRequest.getPrimaryKey());
        out.writeOptionalString(fastReindexRequest.getNestFields());
        out.writeOptionalWriteable(fastReindexRequest.getSourceInfo());
        out.writeOptionalWriteable(fastReindexRequest.getRemoteInfo());
        out.writeOptionalWriteable(fastReindexRequest.getRuleInfo());
    }

    @Override
    public IndicesRequest indices(String... indices) {
        return this;
    }

    @Override
    public String[] indices() {
        return new String[0];
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictExpandOpenAndForbidClosedIgnoreThrottled();
    }
}
