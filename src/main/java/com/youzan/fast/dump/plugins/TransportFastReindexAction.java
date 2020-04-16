package com.youzan.fast.dump.plugins;

import com.google.common.collect.ArrayListMultimap;
import com.youzan.fast.dump.resource.ESFileResource;
import com.youzan.fast.dump.resource.FileResource;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.03.25
 */
public class TransportFastReindexAction extends HandledTransportAction<FastReindexRequest, FastReindexResponse> {

    private TransportNodeFastReindexAction transportNodeFastReindexAction;
    private Client client;

    @Inject
    public TransportFastReindexAction(Settings settings,
                                      ThreadPool threadPool,
                                      ActionFilters actionFilters,
                                      Client client,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      TransportService transportService,
                                      TransportNodeFastReindexAction transportNodeFastReindexAction) {

        super(settings, FastReindexAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                FastReindexRequest::new);
        this.client = client;
        this.transportNodeFastReindexAction = transportNodeFastReindexAction;
    }

    @Override
    protected void doExecute(Task task, final FastReindexRequest request, ActionListener<FastReindexResponse> listener) {
        try {
            long startTime = System.currentTimeMillis();

            FileResource fileResource = new ESFileResource();
            ArrayListMultimap nodeIdFile = fileResource.getSlaveFile(request.getSourceIndex().split(","), client,
                    request.getTargetIndexType(), request.getTargetIndex());

            if (nodeIdFile.size() > 0) {
                final AtomicInteger counter = new AtomicInteger(nodeIdFile.keySet().size());
                final FastReindexResponse fastReindexResponse = new FastReindexResponse();
                nodeIdFile.keySet().forEach(key -> {
                    FastReindexShardRequest fastReindexShardRequest = new FastReindexShardRequest();
                    fastReindexShardRequest.setFastReindexRequest(request);
                    fastReindexShardRequest.setFile(nodeIdFile.get(key));
                    fastReindexResponse.setTotalFile(fastReindexResponse.getTotalFile() + nodeIdFile.get(key).size());
                    fastReindexShardRequest.setNodeId(key.toString());
                    request.setShouldStoreResult(true);
                    transportNodeFastReindexAction.execute(task, fastReindexShardRequest, new ActionListener<FastReindexShardResponse>() {
                        @Override
                        public void onResponse(FastReindexShardResponse fastReindexShardResponse) {
                            fastReindexResponse.getMap().put(key.toString(), fastReindexShardResponse.getFileReadStatusList());
                            if (counter.decrementAndGet() == 0) {
                                finishHim();
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error(e);
                            // create failures for all relevant requests
                            fastReindexResponse.getStatus().put(key.toString(), new FastReindexResponse.ResponseStatus(false, e.toString()));
                            fastReindexResponse.getMap().put(key.toString(), new ArrayList<>());
                            if (counter.decrementAndGet() == 0) {
                                finishHim();
                            }
                        }

                        private void finishHim() {
                            fastReindexResponse.setBuildTime(System.currentTimeMillis() - startTime);
                            listener.onResponse(fastReindexResponse);
                        }
                    });
                });
            }


        } catch (Exception e) {
            listener.onFailure(e);
        }

    }

    @Override
    protected void doExecute(FastReindexRequest request, ActionListener<FastReindexResponse> listener) {
        throw new UnsupportedOperationException("task required");
    }

}
