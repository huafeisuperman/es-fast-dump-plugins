package com.youzan.fast.dump.plugins.speed;

import lombok.Data;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.07.24
 */
@Data
public class FastReindexSpeedResponse extends ActionResponse implements ToXContent {

    private Map<String, FastReindexSpeedNodeResponse> speedAdaptMap = new HashMap<>();

    private long tookTime;

    public FastReindexSpeedResponse() {
    }

    public FastReindexSpeedResponse(StreamInput input) {
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        boolean isSuccess = false;
        builder.startArray("node_status");
        for (String nodeId : speedAdaptMap.keySet()) {
            builder.startObject();
            builder.field("node_id", nodeId);
            builder.field("is_success", speedAdaptMap.get(nodeId).isSuccess());
            builder.field("message", speedAdaptMap.get(nodeId).getMessage());
            builder.field("node_speed", speedAdaptMap.get(nodeId).getNodeSpeed());
            builder.endObject();

            isSuccess = isSuccess | speedAdaptMap.get(nodeId).isSuccess();
        }
        builder.endArray();
        builder.field("success", isSuccess);
        builder.field("took", tookTime);
        builder.endObject();
        return builder;
    }
}
