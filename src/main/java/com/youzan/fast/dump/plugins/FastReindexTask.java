package com.youzan.fast.dump.plugins;

import com.youzan.fast.dump.common.BaseLogger;
import com.youzan.fast.dump.common.StatusEnum;
import com.youzan.fast.dump.common.reader.FileReadStatus;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.03.27
 */
public class FastReindexTask extends CancellableTask implements BaseLogger {

    private long totalFile;

    private List<FileReadStatus> fileReadStatusList = new ArrayList<>();

    private ExecutorService service;

    public void setTotalFile(long totalFile) {
        this.totalFile = totalFile;
    }

    public void setService(ExecutorService service) {
        this.service = service;
    }

    public void setFileReadStatusList(List<FileReadStatus> fileReadStatusList) {
        this.fileReadStatusList = fileReadStatusList;
    }

    public FastReindexTask(long id, String type, String action, String description, TaskId parentTaskId) {
        super(id, type, action, description, parentTaskId, new HashMap<>());
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }


    @Override
    protected void onCancelled() {
        try {
            if (null != service) {
                service.shutdownNow();
            }
        } catch (Exception e) {
            LOGGER.error("executor shutdown error,", e);
        }
    }

    @Override
    public FastReindexTask.Status getStatus() {
        return new FastReindexTask.Status(totalFile, fileReadStatusList);
    }

    public static class Status implements Task.Status {
        public static final String NAME = "fastReindex";

        private long totalFile;

        private long successFile = 0;

        private long failedFile = 0;

        private List<FileReadStatus> processFileInfo = new ArrayList<>();

        public Status(long totalFile, List<FileReadStatus> fileReadStatuses) {
            requireNonNull(fileReadStatuses);
            this.totalFile = totalFile;
            for (FileReadStatus fileReadStatus : fileReadStatuses) {
                if (fileReadStatus.getStatus().equals(StatusEnum.FAILED.getStatus())) {
                    failedFile += 1;
                } else if (fileReadStatus.getStatus().equals(StatusEnum.SUCCESS.getStatus())) {
                    successFile += 1;
                } else if (fileReadStatus.getStatus().equals(StatusEnum.PROCESSING.getStatus())) {
                    long costTime = (System.currentTimeMillis() - fileReadStatus.getStartTime()) / 1000;
                    if (0 == costTime) {
                        costTime = 1;
                    }
                    fileReadStatus.setCurrentRate(fileReadStatus.getCurrentCount() / costTime);
                    processFileInfo.add(fileReadStatus);
                }

            }
        }

        public Status(StreamInput in) throws IOException {
            totalFile = in.readLong();
            successFile = in.readLong();
            failedFile = in.readLong();
            int length = in.readVInt();
            processFileInfo = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                processFileInfo.add(in.readOptionalWriteable(FileReadStatus::new));
            }
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(totalFile);
            out.writeLong(successFile);
            out.writeLong(failedFile);
            out.writeVInt(processFileInfo.size());
            for (FileReadStatus fileReadStatus : processFileInfo) {
                out.writeOptionalWriteable(fileReadStatus);
            }
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            innerXContent(builder, params);
            return builder.endObject();
        }

        public XContentBuilder innerXContent(XContentBuilder builder, Params params)
                throws IOException {
            builder.field("total_file", totalFile);
            builder.field("success_file", successFile);
            builder.field("failed_file", failedFile);
            builder.field("process_file", processFileInfo.size());
            long nodeRate = 0;
            builder.startArray("process_info");
            for (FileReadStatus fileReadStatus : processFileInfo) {
                builder.startObject();
                builder.field("file_path", fileReadStatus.getFilePath());
                builder.field("total_count", fileReadStatus.getTotalCount());
                builder.field("current_count", fileReadStatus.getCurrentCount());
                builder.field("current_rate", fileReadStatus.getCurrentRate());
                nodeRate = nodeRate + fileReadStatus.getCurrentRate();
                builder.endObject();
            }
            builder.endArray();
            builder.field("node_rate", nodeRate);
            return builder;
        }
    }
}
