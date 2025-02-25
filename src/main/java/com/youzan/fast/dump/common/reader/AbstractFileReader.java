package com.youzan.fast.dump.common.reader;

import com.youzan.fast.dump.common.BaseLogger;
import com.youzan.fast.dump.common.IndexModeEnum;
import com.youzan.fast.dump.common.IndexTypeEnum;
import com.youzan.fast.dump.common.rules.Rule;
import com.youzan.fast.dump.plugins.FastReindexRequest;
import com.youzan.fast.dump.plugins.FastReindexTask;
import com.youzan.fast.dump.resolver.DataResolve;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2019.11.20
 */
public abstract class AbstractFileReader implements BaseLogger, FileReader {

    protected ConcurrentLinkedQueue<String> queue;

    protected int batchSize;

    protected int threadNum;

    protected List<FileReadStatus> fileReadList;

    protected int OneFileThreadNum = 10;

    protected boolean isUpdateMode = false;

    protected boolean isCustomType = false;

    protected List<Rule> ruleList = new ArrayList<>();

    protected String targetType;

    private FastReindexTask task;

    public AbstractFileReader(List<String> files, FastReindexTask task) {
        queue = new ConcurrentLinkedQueue<>(files);
        fileReadList = Collections.synchronizedList(new ArrayList<>(files.size()));
        this.task = task;
        task.setTotalFile(files.size());
        task.setFileReadStatusList(fileReadList);
    }

    public AbstractFileReader setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public AbstractFileReader setThreadNum(int threadNum) {
        this.threadNum = threadNum;
        return this;
    }

    public AbstractFileReader setOneFileThreadNum(int oneFileThreadNum) {
        this.OneFileThreadNum = oneFileThreadNum;
        return this;
    }

    public AbstractFileReader setMode(IndexModeEnum mode) {
        if (mode == IndexModeEnum.UPDATE) {
            isUpdateMode = true;
        }
        return this;
    }

    public AbstractFileReader setTargetType(String targetType) {
        this.targetType = targetType;
        return this;
    }

    public AbstractFileReader initRule(String targetIndexType, FastReindexRequest.RuleInfo ruleInfo) throws Exception {
        if (targetIndexType.equals(IndexTypeEnum.CUSTOM.getIndexType())) {
            isCustomType = true;
            String[] fieldArray = ruleInfo.getField().split(":");
            String[] ruleArray = ruleInfo.getRules().split(":");
            String[] classNameArray = ruleInfo.getRuleName().split(":");
            for (int i = 0; i < classNameArray.length; i++) {
                ruleList.add((Rule) Class.forName(classNameArray[i]).getConstructor(String.class, String.class).
                        newInstance(fieldArray[i], ruleArray[i]));
            }
        }
        return this;
    }

    @Override
    public void resolveFile(String file, DataResolve dataResolve, ExecutorService pool) throws Exception {

    }

    @Override
    public void foreachFile(DataResolve dataResolve) throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(threadNum);
        task.setService(service);
        for (int i = 0; i < threadNum; i++) {
            service.submit(() -> {
                String file;
                while (null != (file = queue.poll())) {
                    try {
                        resolveFile(file, dataResolve, Executors.newFixedThreadPool(OneFileThreadNum));
                    } catch (Exception e) {
                        if (e instanceof InterruptedException) {
                            LOGGER.error("task cancel");
                            break;
                        } else {
                            LOGGER.error(e.getMessage(), e);
                            continue;
                        }

                    }
                }
            });
        }
        service.shutdown();
        service.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);
        dataResolve.afterFinish();
    }

    @Override
    public List<FileReadStatus> getFileReadStatusList() {
        return fileReadList;
    }
}
