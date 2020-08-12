package com.youzan.fast.dump.plugins.scan;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.08.07
 */
public class TransportFastReindexScanAction extends HandledTransportAction<FastReindexScanRequest, FastReindexScanResponse> {

    private ClusterService clusterService;
    private TransportNodeFastReindexScanAction transportNodeFastReindexScanAction;

    @Inject
    public TransportFastReindexScanAction(ActionFilters actionFilters,
                                          TransportService transportService,
                                          TransportNodeFastReindexScanAction transportNodeFastReindexScanAction,
                                          ClusterService clusterService) {

        super(FastReindexScanAction.NAME, transportService, actionFilters, FastReindexScanRequest::new);
        this.clusterService = clusterService;
        this.transportNodeFastReindexScanAction = transportNodeFastReindexScanAction;
    }

    @Override
    protected void doExecute(Task task, FastReindexScanRequest request, ActionListener<FastReindexScanResponse> listener) {
        long startTime = System.currentTimeMillis();
        List<String> nodeIds = new ArrayList<>();
        clusterService.state().nodes().getDataNodes().forEach(node -> nodeIds.add(node.key));
        assert nodeIds.size() > 0;
        final AtomicInteger counter = new AtomicInteger(nodeIds.size());
        FastReindexScanResponse response = new FastReindexScanResponse();
        nodeIds.forEach(key -> {
            FastReindexScanRequest scanRequest = new FastReindexScanRequest();
            scanRequest.setId(request.getId());
            scanRequest.setNodeId(key);
            transportNodeFastReindexScanAction.execute(task, scanRequest, new ActionListener<FastReindexScanNodeResponse>() {
                @Override
                public void onResponse(FastReindexScanNodeResponse fastReindexScanNodeResponse) {
                    response.getRecords().addAll(fastReindexScanNodeResponse.getRecords());
                    response.getResultMap().put(key, new FastReindexScanResponse.NodeScanResult(fastReindexScanNodeResponse.isSuccess(),
                            fastReindexScanNodeResponse.getMessage()));
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("scan error", e);
                    response.getResultMap().put(key, new FastReindexScanResponse.NodeScanResult(false, e.getMessage()));
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                private void finishHim() {
                    response.setTookTime(System.currentTimeMillis() - startTime);
                    listener.onResponse(response);
                }
            });
        });
    }
}