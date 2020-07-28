package com.youzan.fast.dump.plugins.speed;

import lombok.Data;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.07.24
 */
@Data
public class FastReindexSpeedRequest extends ActionRequest {

    private String id;

    private int speed;

    private String nodeId;

    public FastReindexSpeedRequest() {

    }

    public FastReindexSpeedRequest(StreamInput input) throws IOException {
        super(input);
        id = input.readString();
        speed = input.readInt();
        nodeId = input.readOptionalString();

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeInt(speed);
        out.writeOptionalString(nodeId);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (speed <= 0) {
            ActionRequestValidationException exception = new ActionRequestValidationException();
            exception.addValidationError("speed can not lte 0");
            return exception;
        }
        return null;
    }
}
