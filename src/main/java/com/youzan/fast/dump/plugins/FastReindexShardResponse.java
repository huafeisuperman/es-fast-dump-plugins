package com.youzan.fast.dump.plugins;

import com.youzan.fast.dump.common.StatusEnum;
import com.youzan.fast.dump.common.reader.FileReadStatus;
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
 * @date: 2020.03.27
 */
@Data
public class FastReindexShardResponse extends ActionResponse implements ToXContent {

    private List<FileReadStatus> fileReadStatusList = new ArrayList<>();

    private String nodeId;

    public FastReindexShardResponse(StreamInput in) throws IOException {
        super(in);
        nodeId = in.readString();
        int length = in.readVInt();
        fileReadStatusList = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            fileReadStatusList.add(in.readOptionalWriteable(FileReadStatus::new));
        }
    }

    public FastReindexShardResponse() {

    }


    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("msg", "ok");
        builder.field("total_speed", "");
        builder.endObject();
        return builder;
    }

    public boolean isFinish() {
        for (FileReadStatus fileReadStatus : fileReadStatusList) {
            if (fileReadStatus.getStatus().equals(StatusEnum.PROCESSING.getStatus())) {
                return false;

            }
        }
        return true;
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeVInt(fileReadStatusList.size());
        for (FileReadStatus fileReadStatus : fileReadStatusList) {
            out.writeOptionalWriteable(fileReadStatus);
        }
    }

}
