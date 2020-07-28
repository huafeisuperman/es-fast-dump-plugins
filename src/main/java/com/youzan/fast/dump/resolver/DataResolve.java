package com.youzan.fast.dump.resolver;

import com.youzan.fast.dump.common.BaseLogger;

/**
 * @author :  43333
 * @Project Name :  mainline
 * @Package Name :  com.youzan.clouddb.tools.general.resolver
 * @Description :  TODO
 * @Creation Date:  2018-08-16 9:47
 * --------  ---------  --------------------------
 */
public interface DataResolve<T> extends BaseLogger {

    default void beforeStart() throws Exception{};

    void resolve(T data) throws Exception;

    void afterFinish() throws Exception;

    void commit() throws Exception;

    void close() throws Exception;

    void changeSpeed(int speed);
}
