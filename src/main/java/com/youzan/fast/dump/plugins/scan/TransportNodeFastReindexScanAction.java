package com.youzan.fast.dump.plugins.scan;

import com.youzan.fast.dump.plugins.TaskIdContext;
import com.youzan.fast.dump.resolver.ScanDataResolve;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
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
 * @date: 2020.08.07
 */
public class TransportNodeFastReindexScanAction extends TransportAction<FastReindexScanRequest, FastReindexScanNodeResponse> {
    public static final String ACTION_NAME = FastReindexScanAction.NAME + "[n]";

    private TransportService transportService;

    private final ClusterService clusterService;

    @Inject
    public TransportNodeFastReindexScanAction(ClusterService clusterService,
                                              ActionFilters actionFilters,
                                              TransportService transportService) {
        super(ACTION_NAME, actionFilters, transportService.getTaskManager());
        this.transportService = transportService;
        transportService.registerRequestHandler(actionName, ThreadPool.Names.GENERIC, FastReindexScanRequest::new, new TransportNodeFastReindexScanAction.NodeOperationTransportHandler());
        this.clusterService = clusterService;
    }


    @Override
    protected void doExecute(Task task, FastReindexScanRequest request, ActionListener<FastReindexScanNodeResponse> listener) {
        new TransportNodeFastReindexScanAction.NodeReroutePhase(request, listener).run();
    }


    final class NodeReroutePhase extends AbstractRunnable {
        private final ActionListener<FastReindexScanNodeResponse> listener;
        private final FastReindexScanRequest request;
        private final AtomicBoolean finished = new AtomicBoolean();

        NodeReroutePhase(FastReindexScanRequest request, ActionListener<FastReindexScanNodeResponse> listener) {
            this.request = request;
            this.listener = listener;
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
            transportService.sendRequest(node, action, requestToPerform, new TransportResponseHandler<FastReindexScanNodeResponse>() {

                @Override
                public FastReindexScanNodeResponse read(StreamInput in) throws IOException {
                    return new FastReindexScanNodeResponse(in);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public void handleResponse(FastReindexScanNodeResponse response) {
                    finishOnSuccess(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    try {
                        // if we got disconnected from the node, or the node is not in the right state (being closed)
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

        void finishOnSuccess(FastReindexScanNodeResponse response) {
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


    class NodeOperationTransportHandler implements TransportRequestHandler<FastReindexScanRequest> {


        @Override
        public void messageReceived(FastReindexScanRequest request, TransportChannel channel, Task task) throws Exception {
            new TransportNodeFastReindexScanAction.AsyncNodeAction(request, channel).run();
        }

    }

    private final class AsyncNodeAction extends AbstractRunnable implements ActionListener<Releasable> {
        private final FastReindexScanRequest request;

        private final TransportChannel channel;


        AsyncNodeAction(FastReindexScanRequest request, TransportChannel channel) {
            this.request = request;
            this.channel = channel;
        }

        @Override
        public void onResponse(Releasable releasable) {
            try {
                FastReindexScanNodeResponse response = new FastReindexScanNodeResponse();
                TaskIdContext.ResolveSpeed dataResolve = TaskIdContext.get(request.getId());
                response.setSuccess(true);
                if (dataResolve != null) {
                    response.getRecords().addAll(((ScanDataResolve) dataResolve.getDataResolve()).getRecords());
                    dataResolve.setCurrentTime(System.currentTimeMillis());
                } else {
                    response.setMessage("scan has no message");
                }
                TransportNodeFastReindexScanAction.AsyncNodeAction.ResponseListener rl = new TransportNodeFastReindexScanAction.AsyncNodeAction.ResponseListener();
                rl.onResponse(response);
            } catch (Exception e) {
                Releasables.closeWhileHandlingException(releasable); // release node operation lock before responding to caller
                TransportNodeFastReindexScanAction.AsyncNodeAction.this.onFailure(e);
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
        protected void doRun() {
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
                    logger.trace("id [{}] completed on node", request.getId());
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

