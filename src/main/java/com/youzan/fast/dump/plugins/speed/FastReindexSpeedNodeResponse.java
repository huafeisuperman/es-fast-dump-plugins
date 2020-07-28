package com.youzan.fast.dump.plugins.speed;

import lombok.Data;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.07.25
 */
@Data
public class FastReindexSpeedNodeResponse extends ActionResponse implements ToXContent {

    private boolean isSuccess = false;

    private String message = "ok";

    private int nodeSpeed;

    public FastReindexSpeedNodeResponse() {
    }

    public FastReindexSpeedNodeResponse(StreamInput input) throws IOException {
        super(input);
        isSuccess = input.readBoolean();
        message = input.readString();
        nodeSpeed = input.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isSuccess);
        out.writeString(message);
        out.writeInt(nodeSpeed);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }
}
