package com.youzan.fast.dump.plugins;

import com.youzan.fast.dump.common.StatusEnum;
import com.youzan.fast.dump.common.reader.FileReadStatus;
import lombok.Builder;
import lombok.Data;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.03.25
 */
@Data
public class FastReindexResponse extends ActionResponse implements ToXContent {

    private Map<String, List<FileReadStatus>> map = new ConcurrentHashMap<>();

    private Map<String, ResponseStatus> status = new ConcurrentHashMap<>();

    int totalFile = 0;

    private long buildTime;


    public FastReindexResponse(StreamInput in) {
    }

    public FastReindexResponse() {
    }


    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        int successFile = 0;
        int failedFile = 0;
        long totalCount = 0;
        builder.startArray("node_status");
        for (String s : map.keySet()) {
            builder.startObject();
            builder.field("node_id", s);
            builder.startArray("file_status");
            boolean success = true;
            for (FileReadStatus readStatus : map.get(s)) {
                builder.startObject();
                builder.field("file_path", readStatus.getFilePath());
                builder.field("total_count", readStatus.getTotalCount());
                totalCount = totalCount + readStatus.getTotalCount();
                builder.field("status", StatusEnum.findStatusEnum(readStatus.getStatus()));
                builder.field("msg", readStatus.getMsg());
                if (readStatus.getStatus().equals(StatusEnum.FAILED.getStatus())) {
                    success = false;
                    failedFile = failedFile + 1;
                } else {
                    successFile = successFile + 1;
                }
                builder.endObject();
            }
            builder.endArray();

            if (null != status.get(s)) {
                builder.field("success", status.get(s).status);
                builder.field("msg", status.get(s).message);
            } else {
                builder.field("success", success);
                builder.field("msg", "ok");
            }

            builder.endObject();
        }
        builder.endArray();

        builder.field("total_count", totalCount);
        builder.field("total_success", totalFile == successFile ? true : false);
        builder.field("total_file", totalFile);
        builder.field("success_file", successFile);
        builder.field("failed_file", failedFile);
        long costSecond = buildTime / 1000;
        builder.field("total_rate", totalCount / (costSecond == 0 ? 1 : costSecond));
        builder.field("cost", buildTime);
        return builder;
    }

    /**
     * 返回数据
     *
     * @param builder 构造器
     * @return 返回一个json
     * @throws IOException 异常
     */
    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        builder.startObject();
        toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    @Data
    @Builder
    public static class ResponseStatus {
        private boolean status;
        private String message;
    }




}
