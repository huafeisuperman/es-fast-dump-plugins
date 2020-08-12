package com.youzan.fast.dump.plugins.scan;

import lombok.Data;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.08.07
 */
@Data
public class FastReindexScanNodeResponse extends ActionResponse implements ToXContent {

    private boolean isSuccess = false;

    private String message = "ok";

    private List<String> records = new ArrayList<>();

    public FastReindexScanNodeResponse() {
    }

    public FastReindexScanNodeResponse(StreamInput input) throws IOException {
        super(input);
        isSuccess = input.readBoolean();
        int length = input.readInt();
        for (int i = 0; i < length; i++) {
            records.add(input.readString());
        }
        message = input.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isSuccess);
        out.writeInt(records.size());
        for (String record : records) {
            out.writeString(record);
        }
        out.writeString(message);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) {
        return null;
    }
}

