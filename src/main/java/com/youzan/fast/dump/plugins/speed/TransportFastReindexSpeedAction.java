package com.youzan.fast.dump.plugins.speed;

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
 * @date: 2020.07.24
 */
public class TransportFastReindexSpeedAction extends HandledTransportAction<FastReindexSpeedRequest, FastReindexSpeedResponse> {

    private ClusterService clusterService;
    private TransportNodeFastReindexSpeedAction transportNodeFastReindexSpeedAction;

    @Inject
    public TransportFastReindexSpeedAction(ActionFilters actionFilters,
                                           TransportService transportService,
                                           TransportNodeFastReindexSpeedAction transportNodeFastReindexSpeedAction,
                                           ClusterService clusterService) {

        super(FastReindexSpeedAction.NAME, transportService, actionFilters, FastReindexSpeedRequest::new);
        this.clusterService = clusterService;
        this.transportNodeFastReindexSpeedAction = transportNodeFastReindexSpeedAction;
    }

    @Override
    protected void doExecute(Task task, FastReindexSpeedRequest request, ActionListener<FastReindexSpeedResponse> listener) {
        long startTime = System.currentTimeMillis();
        List<String> nodeIds = new ArrayList<>();
        clusterService.state().nodes().getDataNodes().forEach(node -> nodeIds.add(node.key));
        assert nodeIds.size() > 0;
        final AtomicInteger counter = new AtomicInteger(nodeIds.size());
        FastReindexSpeedResponse response = new FastReindexSpeedResponse();
        nodeIds.forEach(key -> {
            FastReindexSpeedRequest reindexSpeedNodeRequest = new FastReindexSpeedRequest();
            reindexSpeedNodeRequest.setSpeed(request.getSpeed());
            reindexSpeedNodeRequest.setId(request.getId());
            reindexSpeedNodeRequest.setNodeId(key);
            transportNodeFastReindexSpeedAction.execute(task, reindexSpeedNodeRequest, new ActionListener<FastReindexSpeedNodeResponse>() {
                @Override
                public void onResponse(FastReindexSpeedNodeResponse fastReindexSpeedNodeResponse) {
                    response.getSpeedAdaptMap().put(key, fastReindexSpeedNodeResponse);
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("change speed error", e);
                    // create failures for all relevant requests
                    FastReindexSpeedNodeResponse reindexSpeedNodeResponse = new FastReindexSpeedNodeResponse();
                    reindexSpeedNodeResponse.setSuccess(false);
                    reindexSpeedNodeResponse.setMessage(e.getMessage());
                    response.getSpeedAdaptMap().put(key, reindexSpeedNodeResponse);
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
