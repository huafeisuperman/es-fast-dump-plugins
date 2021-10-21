package com.youzan.fast.dump.plugins;

import com.youzan.fast.dump.resolver.DataResolve;
import com.youzan.fast.dump.resolver.ScanDataResolve;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.07.25
 */
public class TaskIdContext implements Runnable {

    protected static Logger logger = LogManager.getLogger(TaskIdContext.class);

    private static Map<String, ResolveSpeed> idResolve = new HashMap<>();

    private static ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    public static synchronized void put(String id, ResolveSpeed resolve) {
        idResolve.put(id, resolve);
    }

    public static synchronized void remove(String id) {
        idResolve.remove(id);
    }

    public static synchronized ResolveSpeed get(String id) {
        return idResolve.get(id);
    }

    static {
        executorService.scheduleAtFixedRate(new ScanTimeExpire(), 0, 10, TimeUnit.SECONDS);
    }

    @Override
    public void run() {

    }

    @Data
    @AllArgsConstructor
    public static class ResolveSpeed {
        public int totalNodeSize;
        public DataResolve dataResolve;
        private long currentTime;
        private FastReindexTask task;

        public int changeSpeed(int speed) {
            int nodeSpeed = speed / totalNodeSize;
            if (nodeSpeed > 0) {
                dataResolve.changeSpeed(speed);
            }
            return nodeSpeed;
        }
    }

    public static class ScanTimeExpire implements Runnable {

        @Override
        public void run() {
            try {
                List<String> needRemoveId = new ArrayList<>();
                idResolve.forEach((x, y) -> {
                    if (y.getDataResolve() instanceof ScanDataResolve) {
                        if ((System.currentTimeMillis() - y.getCurrentTime()) > 60000) {
                            logger.info("task id[{}] expired", x);
                            y.getTask().onCancelled();
                            needRemoveId.add(x);
                        }
                    }
                });
                needRemoveId.forEach(x -> remove(x));
            } catch (Exception e) {
                logger.error("scan task expire error,", e);
            }
        }
    }


}
