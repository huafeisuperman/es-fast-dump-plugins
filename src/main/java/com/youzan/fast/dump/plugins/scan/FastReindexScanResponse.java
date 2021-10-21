package com.youzan.fast.dump.plugins.scan;

import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.08.07
 */
@Data
public class FastReindexScanResponse extends ActionResponse implements ToXContent {

    private long tookTime;

    private List<String> records = Collections.synchronizedList(new ArrayList<>());

    private Map<String, NodeScanResult> resultMap = new HashMap<>();

    public FastReindexScanResponse() {
    }

    public FastReindexScanResponse(StreamInput input) {
    }

    @Override
    public void writeTo(StreamOutput out) {

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("took", tookTime);
        boolean success = true;
        String errorMessage = null;

        builder.startArray("node_error");
        for (String key : resultMap.keySet()) {



            if (resultMap.get(key).success) {
                continue;
            }
            success = false;
            builder.startObject();
            builder.field("node_id", key);
            builder.field("success", resultMap.get(key).success);
            builder.field("message", resultMap.get(key).message);
            builder.endObject();
        }
        builder.endArray();
        builder.field("success", success);
        builder.field("hits", records.stream().map(record -> JSONObject.parseObject(record)).collect(Collectors.toList()));

        builder.endObject();
        return builder;
    }

    @AllArgsConstructor
    public static class NodeScanResult {
        private boolean success;

        private String message;
    }

}
