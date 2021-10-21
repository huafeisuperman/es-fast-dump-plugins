package com.youzan.fast.dump.plugins.scan;

import lombok.Data;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.08.07
 */
@Data
public class FastReindexScanRequest extends ActionRequest implements IndicesRequest.Replaceable {

    private String id;

    private String nodeId;

    private int scanSize;

    public FastReindexScanRequest() {

    }

    public FastReindexScanRequest(StreamInput input) throws IOException {
        super(input);
        id = input.readString();
        nodeId = input.readString();
        scanSize = input.readInt();

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeString(nodeId);
        out.writeInt(scanSize);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (scanSize <= 0) {
            ActionRequestValidationException exception = new ActionRequestValidationException();
            exception.addValidationError("scan size can not lte 0");
            return exception;
        }
        return null;
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
