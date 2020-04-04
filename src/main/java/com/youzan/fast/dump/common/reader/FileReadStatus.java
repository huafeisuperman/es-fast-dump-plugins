package com.youzan.fast.dump.common.reader;

import com.youzan.fast.dump.common.StatusEnum;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author :  43333
 * @Project Name :  mainline
 * @Package Name :  com.youzan.clouddb.tools.general.common
 * @Description :  TODO
 * @Creation Date:  2018-08-15 14:21
 * --------  ---------  --------------------------
 */
public class FileReadStatus implements Writeable {
    //文件路径
    private String filePath;
    //总共记录数
    private long totalCount;
    //当前处理的记录数
    private long currentCount;
    //时间间隔内处理的记录速率
    private long currentRate;
    //文件状态
    private volatile String status;
    //开始处理时间
    private long startTime;
    //文件处理结果信息
    private String msg = "ok";

    public FileReadStatus(String filePath, long totalCount, long currentCount) {
        this.filePath = filePath;
        this.totalCount = totalCount;
        this.currentCount = currentCount;
        this.status = StatusEnum.PROCESSING.getStatus();
        this.startTime = System.currentTimeMillis();
    }


    public String getFilePath() {
        return filePath;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public long getCurrentCount() {
        return currentCount;
    }

    public synchronized void setCurrentCount(long currentCount) {
        this.currentCount = currentCount;
    }

    public long getCurrentRate() {
        return currentRate;
    }

    public void setCurrentRate(long currentRate) {
        this.currentRate = currentRate;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getStartTime() {
        return startTime;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    /**
     * Read from a stream.
     */
    public FileReadStatus(StreamInput in) throws IOException {
        filePath = in.readString();
        totalCount = in.readLong();
        currentCount = in.readLong();
        currentRate = in.readLong();
        status = in.readString();
        startTime = in.readLong();
        msg = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(filePath);
        out.writeLong(totalCount);
        out.writeLong(currentCount);
        out.writeLong(currentRate);
        out.writeString(status);
        out.writeLong(startTime);
        out.writeString(msg);
    }
}
