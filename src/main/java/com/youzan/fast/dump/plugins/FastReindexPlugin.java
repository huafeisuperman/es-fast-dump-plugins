package com.youzan.fast.dump.plugins;

import com.youzan.fast.dump.plugins.scan.FastReindexScanAction;
import com.youzan.fast.dump.plugins.scan.FastReindexScanRestHandler;
import com.youzan.fast.dump.plugins.scan.TransportFastReindexScanAction;
import com.youzan.fast.dump.plugins.speed.FastReindexSpeedAction;
import com.youzan.fast.dump.plugins.speed.FastReindexSpeedRestHandler;
import com.youzan.fast.dump.plugins.speed.TransportFastReindexSpeedAction;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.03.25
 */
public class FastReindexPlugin extends Plugin implements ActionPlugin {


    public static final String NAME = "fast_reindex";

    public static final String FAST_REINDEX_SIZE_KEY = "thread_pool.fast_reindex.size";

    public static final String FAST_REINDEX_QUEUE_SIZE_KEY = "thread_pool.fast_reindex.queue_size";

    public static final String FAST_REINDEX_THREAD_POOL_NAME = NAME;

    public static final int FAST_REINDEX_THREAD_POOL_SIZE = 1;

    public static final int FAST_REINDEX_THREAD_POOL_QUEUE_SIZE = 1;

    public static final Setting<Integer> SIZE = Setting.intSetting(FAST_REINDEX_SIZE_KEY, FAST_REINDEX_THREAD_POOL_SIZE, Setting.Property.NodeScope);

    public static final Setting<Integer> QUEUE_SIZE = Setting.intSetting(FAST_REINDEX_QUEUE_SIZE_KEY, FAST_REINDEX_THREAD_POOL_QUEUE_SIZE, Setting.Property.NodeScope);

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {

        return Arrays.asList(new ActionHandler(FastReindexAction.INSTANCE, TransportFastReindexAction.class),
                new ActionHandler(FastReindexSpeedAction.INSTANCE, TransportFastReindexSpeedAction.class),
                new ActionHandler(FastReindexScanAction.INSTANCE, TransportFastReindexScanAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new FastReindexRestHandler(restController),
                new FastReindexSpeedRestHandler(restController),
                new FastReindexScanRestHandler(restController));
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Collections.singletonList(new NamedWriteableRegistry.Entry(Task.Status.class, FastReindexTask.Status.NAME, FastReindexTask.Status::new));
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return Collections.singletonList(new FixedExecutorBuilder(settings, FAST_REINDEX_THREAD_POOL_NAME,
                settings.getAsInt(FAST_REINDEX_SIZE_KEY, FAST_REINDEX_THREAD_POOL_SIZE),
                settings.getAsInt(FAST_REINDEX_QUEUE_SIZE_KEY, FAST_REINDEX_THREAD_POOL_QUEUE_SIZE),
                "fast_reindex_thread_pool"));
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settingList = new ArrayList<>();
        settingList.add(SIZE);
        settingList.add(QUEUE_SIZE);
        return settingList;
    }
}
