package com.youzan.fast.dump.resolver;

import com.alibaba.fastjson.JSONObject;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.08.07
 */
public class ScanDataResolve extends BaseDataResolve {

    private BlockingQueue<JSONObject> messagesQueue = new LinkedBlockingQueue<>(20000);

    private boolean finish = false;

    public ScanDataResolve(int speedLimit) {
        super(speedLimit);
    }

    @Override
    public void resolve(List<JSONObject> data) throws Exception {
        rateLimiter.acquire(data.size());
        for (JSONObject record : data) {
            messagesQueue.put(record);
        }
    }

    @Override
    public void close() {
        finish = true;
    }

    public List<String> getRecords(int scanSize) {
        List<String> js = new ArrayList<>();
        AccessController.doPrivileged(
                (PrivilegedAction<Void>) () -> {
                    try {
                        for (int i = 0; i < scanSize; i++) {
                            if (finish && messagesQueue.isEmpty()) break;
                            JSONObject record = messagesQueue.poll(100, TimeUnit.MILLISECONDS);
                            while (record == null && finish == false) {
                                record = messagesQueue.poll(1, TimeUnit.SECONDS);
                            }
                            if (null != record) {
                                js.add(record.toString());
                            }
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException("take message error", e);
                    }
                    return null;
                });
        return js;
    }
}
