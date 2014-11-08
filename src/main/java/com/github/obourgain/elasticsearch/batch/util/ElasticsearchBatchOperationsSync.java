package com.github.obourgain.elasticsearch.batch.util;

import java.util.Map;
import org.elasticsearch.client.Client;

public class ElasticsearchBatchOperationsSync {

    private final ElasticsearchBatchOperationsAsync operationsAsync;

    public ElasticsearchBatchOperationsSync(Client client) {
        operationsAsync = new ElasticsearchBatchOperationsAsync(client);
    }

    public void refresh(String... indices) {
        operationsAsync.refresh(indices).actionGet();
    }

    public void disableRefresh(String... indices) {
        operationsAsync.disableRefresh(indices).actionGet();
    }

    public String getRefreshInterval(String index) {
        return operationsAsync.getRefreshInterval(index).actionGet();
    }

    public void setRefreshInterval(String refreshInterval, String... indices) {
        operationsAsync.setRefreshInterval(refreshInterval, indices).actionGet();
    }

    public void disableReplicas(String... indices) {
        operationsAsync.disableReplicas(indices).actionGet();
    }

    public void setReplicas(int replicas, String... indices) {
        operationsAsync.setReplicas(replicas, indices).actionGet();
    }

    public void updateSettings(Map<String, String> settings, String... indices) {
        operationsAsync.updateSettings(settings, indices).actionGet();
    }

    public void switchAlias(String alias, String fromIndex, String toIndex) {
        operationsAsync.switchAlias(alias, fromIndex, toIndex).actionGet();
    }

}
