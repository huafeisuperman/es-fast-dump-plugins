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

    public FastReindexScanRequest() {

    }

    public FastReindexScanRequest(StreamInput input) throws IOException {
        super(input);
        id = input.readString();
        nodeId = input.readString();

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeString(nodeId);
    }

    @Override
    public ActionRequestValidationException validate() {
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
