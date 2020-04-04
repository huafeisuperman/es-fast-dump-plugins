package com.youzan.fast.dump.common.reader;

import com.youzan.fast.dump.resolver.DataResolve;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author :  43333
 * @Project Name :  mainline
 * @Package Name :  com.youzan.clouddb.tools.general.common
 * @Description :  TODO
 * @Creation Date:  2018-08-16 10:58
 * --------  ---------  --------------------------
 */
public interface FileReader {

    /**
     * 处理文件里的记录
     *
     * @param file        记录文件
     * @param dataResolve 处理器
     * @throws Exception 异常
     */
    void resolveFile(String file, DataResolve dataResolve, ExecutorService pool) throws Exception;

    /**
     * 遍历文件
     *
     * @param dataResolve 处理器
     * @throws Exception 异常
     */
    void foreachFile(final DataResolve dataResolve) throws Exception;

    List<FileReadStatus> getFileReadStatusList();
}
