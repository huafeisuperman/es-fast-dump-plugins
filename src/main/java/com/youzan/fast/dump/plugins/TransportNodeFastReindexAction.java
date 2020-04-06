package com.youzan.fast.dump.plugins;

import com.youzan.fast.dump.common.IndexModeEnum;
import com.youzan.fast.dump.common.reader.FileReader;
import com.youzan.fast.dump.common.reader.LuceneFileReader;
import com.youzan.fast.dump.resolver.DataResolve;
import com.youzan.fast.dump.resolver.DataResolveFactory;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.03.27
 */
public class TransportNodeFastReindexAction extends TransportAction<FastReindexShardRequest, FastReindexShardResponse> {

    public static final String ACTION_NAME = FastReindexAction.NAME + "[s]";

    private TransportService transportService;

    private final ClusterService clusterService;

    private Client client;


    @Inject
    public TransportNodeFastReindexAction(Settings settings,
                                          ThreadPool threadPool,
                                          ClusterService clusterService,
                                          ActionFilters actionFilters,
                                          Client client,
                                          IndexNameExpressionResolver indexNameExpressionResolver,
                                          TransportService transportService) {
        super(settings, ACTION_NAME, threadPool, actionFilters,
                indexNameExpressionResolver, transportService.getTaskManager());
        this.transportService = transportService;
        this.client = client;
        transportService.registerRequestHandler(actionName, FastReindexShardRequest::new, ThreadPool.Names.GENERIC, new ShardOperationTransportHandler());
        this.clusterService = clusterService;
    }


    @Override
    protected final void doExecute(FastReindexShardRequest request, ActionListener<FastReindexShardResponse> listener) {
        throw new UnsupportedOperationException("the task parameter is required for this operation");
    }

    @Override
    protected void doExecute(Task task, FastReindexShardRequest request, ActionListener<FastReindexShardResponse> listener) {
        new ShardReroutePhase((FastReindexTask) task, request, listener).run();
    }


    final class ShardReroutePhase extends AbstractRunnable {
        private final ActionListener<FastReindexShardResponse> listener;
        private final FastReindexShardRequest request;
        private final FastReindexTask task;
        private final AtomicBoolean finished = new AtomicBoolean();

        ShardReroutePhase(FastReindexTask task, FastReindexShardRequest request, ActionListener<FastReindexShardResponse> listener) {
            this.request = request;
            if (task != null) {
                this.request.setParentTask(clusterService.localNode().getId(), task.getId());
            }
            this.listener = listener;
            this.task = task;
        }

        @Override
        public void onFailure(Exception e) {
            finishWithUnexpectedFailure(e);
        }

        @Override
        protected void doRun() {
            final DiscoveryNode node = clusterService.state().nodes().get(request.getNodeId());
            performAction(node, ACTION_NAME, request);
        }

        private void performAction(final DiscoveryNode node, final String action,
                                   final TransportRequest requestToPerform) {
            transportService.sendRequest(node, action, requestToPerform, new TransportResponseHandler<FastReindexShardResponse>() {

                @Override
                public FastReindexShardResponse newInstance() {
                    return newResponseInstance();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public void handleResponse(FastReindexShardResponse response) {
                    finishOnSuccess(response);
                }

                protected FastReindexShardResponse newResponseInstance() {
                    return new FastReindexShardResponse();
                }

                @Override
                public void handleException(TransportException exp) {
                    try {
                        // if we got disconnected from the node, or the node / shard is not in the right state (being closed)
                        finishAsFailed(exp);
                    } catch (Exception e) {
                        e.addSuppressed(exp);
                        finishWithUnexpectedFailure(e);
                    }
                }
            });
        }

        void finishWithUnexpectedFailure(Exception failure) {
            logger.error(
                    (org.apache.logging.log4j.util.Supplier<?>)
                            () -> new ParameterizedMessage(
                                    "unexpected error during the primary phase for action [{}], request [{}]",
                                    actionName,
                                    request),
                    failure);
            if (finished.compareAndSet(false, true)) {
                listener.onFailure(failure);
            } else {
                assert false : "finishWithUnexpectedFailure called but operation is already finished";
            }
        }


        void finishAsFailed(Exception failure) {
            if (finished.compareAndSet(false, true)) {
                logger.error(
                        (org.apache.logging.log4j.util.Supplier<?>)
                                () -> new ParameterizedMessage("operation failed. action [{}], request [{}]", actionName, request), failure);
                listener.onFailure(failure);
            } else {
                assert false : "finishAsFailed called but operation is already finished";
            }
        }

        void finishOnSuccess(FastReindexShardResponse response) {
            if (finished.compareAndSet(false, true)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("operation succeeded. action [{}],request [{}]", actionName, request);
                }
                listener.onResponse(response);
            } else {
                assert false : "finishOnSuccess called but operation is already finished";
            }
        }


    }


    class ShardOperationTransportHandler implements TransportRequestHandler<FastReindexShardRequest> {


        @Override
        public void messageReceived(FastReindexShardRequest request, TransportChannel channel, Task task) throws Exception {
            new AsyncShardAction(request, channel, (FastReindexTask) task).run();
        }

        @Override
        public void messageReceived(FastReindexShardRequest request, TransportChannel channel) throws Exception {
            throw new UnsupportedOperationException("the task parameter is required for this operation");
        }
    }

    private final class AsyncShardAction extends AbstractRunnable implements ActionListener<Releasable> {
        private final FastReindexShardRequest request;
        /**
         * The task on the node with the shard.
         */
        private final FastReindexTask task;

        private final TransportChannel channel;


        AsyncShardAction(FastReindexShardRequest request, TransportChannel channel,
                         FastReindexTask task) {
            this.request = request;
            this.task = task;
            this.channel = channel;
        }

        @Override
        public void onResponse(Releasable releasable) {
            DataResolve resolve = null;
            try {
                FastReindexShardResponse response = new FastReindexShardResponse();
                response.setNodeId(request.getNodeId());
                resolve = DataResolveFactory.getDataResolve(request, client);
                FileReader fileReader = new LuceneFileReader(request.getFile(), task).
                        setBatchSize(request.getFastReindexRequest().getBatchSize()).setThreadNum(request.getFastReindexRequest().getThreadNum()).
                        setQuery(request.getFastReindexRequest().getQuery()).
                        setOneFileThreadNum(request.getFastReindexRequest().getOneFileThreadNum()).
                        setMode(IndexModeEnum.findModeEnum(request.getFastReindexRequest().getMode())).
                        initRule(request.getFastReindexRequest().getTargetIndexType(),
                                request.getFastReindexRequest().getRuleInfo());

                fileReader.foreachFile(resolve);
                response.setFileReadStatusList(fileReader.getFileReadStatusList());
                ResponseListener rl = new TransportNodeFastReindexAction.AsyncShardAction.ResponseListener();
                rl.onResponse(response);
            } catch (Exception e) {
                Releasables.closeWhileHandlingException(releasable); // release shard operation lock before responding to caller
                TransportNodeFastReindexAction.AsyncShardAction.this.onFailure(e);
            } finally {
                try {
                    if (null != resolve) {
                        resolve.close();
                    }
                } catch (Exception e) {
                    logger.error("close client error", e);
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            responseWithFailure(e);
        }

        protected void responseWithFailure(Exception e) {
            try {
                channel.sendResponse(e);
            } catch (IOException responseException) {
                responseException.addSuppressed(e);
                logger.warn(
                        (org.apache.logging.log4j.util.Supplier<?>)
                                () -> new ParameterizedMessage(
                                        "failed to send error message back to client for action [{}]",
                                        ACTION_NAME),
                        responseException);
            }
        }

        @Override
        protected void doRun() throws Exception {
            this.onResponse(() -> {
            });
        }


        /**
         * Listens for the response on the replica and sends the response back to the primary.
         */
        private class ResponseListener implements ActionListener<TransportResponse> {
            @Override
            public void onResponse(TransportResponse response) {
                if (logger.isTraceEnabled()) {
                    logger.trace("action [{}] completed on shard [{}] for request [{}]", request, request.getFile(),
                            request);
                }
                try {
                    channel.sendResponse(response);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                responseWithFailure(e);
            }
        }
    }
}
