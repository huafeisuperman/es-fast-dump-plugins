package com.youzan.fast.dump.plugins;

import com.google.common.collect.ArrayListMultimap;
import com.youzan.fast.dump.common.StatusEnum;
import com.youzan.fast.dump.common.reader.FileReadStatus;
import com.youzan.fast.dump.resource.ESFileResource;
import com.youzan.fast.dump.resource.FileResource;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.LoggingTaskListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.03.25
 */
public class TransportFastReindexAction extends HandledTransportAction<FastReindexRequest, FastReindexResponse> {

    private ClusterService clusterService;
    private TransportNodeFastReindexAction transportNodeFastReindexAction;
    private long startTime;

    @Inject
    public TransportFastReindexAction(Settings settings,
                                      ThreadPool threadPool,
                                      ClusterService clusterService,
                                      ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      TransportService transportService,
                                      TransportNodeFastReindexAction transportNodeFastReindexAction) {

        super(settings, FastReindexAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                FastReindexRequest::new);
        this.clusterService = clusterService;
        this.transportNodeFastReindexAction = transportNodeFastReindexAction;
    }

    @Override
    protected void doExecute(Task task, final FastReindexRequest request, ActionListener<FastReindexResponse> listener) {
        try {
            startTime = System.currentTimeMillis();
            String clusterName = clusterService.getClusterName().value();
            String ip = clusterService.localNode().getHostAddress();
            String port = clusterService.getSettings().get("transport.tcp.port");
            if (null == port) {
                port = "9300";
            }
            FileResource fileResource = new ESFileResource();
            ArrayListMultimap nodeIdFile = fileResource.getSlaveFile(request.getSourceIndex().split(","), ip, port, clusterName, request.getTargetIndexType(), request.getTargetIndex());

            if (nodeIdFile.size() > 0) {
                final AtomicInteger counter = new AtomicInteger(nodeIdFile.keySet().size());
                final FastReindexResponse fastReindexResponse = new FastReindexResponse();
                nodeIdFile.keySet().forEach(key -> {
                    FastReindexShardRequest fastReindexShardRequest = new FastReindexShardRequest();
                    fastReindexShardRequest.setFastReindexRequest(request);
                    fastReindexShardRequest.setFile(nodeIdFile.get(key));
                    fastReindexResponse.setTotalFile(fastReindexResponse.getTotalFile() + nodeIdFile.get(key).size());
                    fastReindexShardRequest.setNodeId(key.toString());
                    logger.info(counter.get() + "......" + key.toString() + fastReindexShardRequest);
                    request.setShouldStoreResult(true);
                    transportNodeFastReindexAction.execute(task, fastReindexShardRequest, new ActionListener<FastReindexShardResponse>() {
                        @Override
                        public void onResponse(FastReindexShardResponse fastReindexShardResponse) {
                            boolean isFinish = true;
                            logger.info(key.toString() + "   " + counter.get());
                            logger.info("ppppppp" + fastReindexShardResponse.getFileReadStatusList().size());
                            fastReindexResponse.getMap().put(key.toString(), fastReindexShardResponse.getFileReadStatusList());
                            if (counter.decrementAndGet() == 0) {
                                finishHim();
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.info(key.toString() + "  dddd " + counter.get());
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
