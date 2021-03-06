package com.github.obourgain.elasticsearch.batch.util;

import static org.slf4j.LoggerFactory.getLogger;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.support.AdapterActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;

public class ElasticsearchBatchOperationsAsync {

    private static final Logger logger = getLogger(ElasticsearchBatchOperationsAsync.class);

    private final Client client;

    public ElasticsearchBatchOperationsAsync(Client client) {
        this.client = client;
    }

    public ActionFuture<RefreshResponse> refresh(String... indices) {
        logger.trace("Refreshing {}", Arrays.asList(indices));
        return client.admin().indices().refresh(Requests.refreshRequest(indices));
    }

    public ActionFuture<UpdateSettingsResponse> disableRefresh(String... indices) {
        logger.trace("Disabling refresh on {}", Arrays.asList(indices));
        return updateSettings(Collections.singletonMap("refresh_interval", "-1"), indices);
    }

    public ActionFuture<String> getRefreshInterval(final String index) {
        AdapterActionFuture<String, GetSettingsResponse> future = new AdapterActionFuture<String, GetSettingsResponse>() {
            @Override
            protected String convert(GetSettingsResponse listenerResponse) {
                return listenerResponse.getSetting(index, "index.refresh_interval");
            }
        };
        client.admin().indices().getSettings(new GetSettingsRequest().indices(index), future);
        return future;
    }

    public ActionFuture<UpdateSettingsResponse> setRefreshInterval(String refreshInterval, String... indices) {
        logger.trace("Set refresh to {} on {}", refreshInterval, Arrays.asList(indices));
        return updateSettings(Collections.singletonMap("refresh_interval", refreshInterval), indices);
    }

    public ActionFuture<UpdateSettingsResponse> disableReplicas(String... indices) {
        logger.trace("Disabling replicas on {}", Arrays.asList(indices));
        return updateSettings(Collections.singletonMap("number_of_replicas", "0"), indices);
    }

    public ActionFuture<UpdateSettingsResponse> setReplicas(int replicas, String... indices) {
        logger.trace("Set replicas to {} on {}", replicas, Arrays.asList(indices));
        return updateSettings(Collections.singletonMap("number_of_replicas", String.valueOf(replicas)), indices);
    }

    public ActionFuture<Integer> getReplicas(final String index) {
        AdapterActionFuture<Integer, GetSettingsResponse> future = new AdapterActionFuture<Integer, GetSettingsResponse>() {
            @Override
            protected Integer convert(GetSettingsResponse listenerResponse) {
                return Integer.valueOf(listenerResponse.getSetting(index, "index.refresh_interval"));
            }
        };
        client.admin().indices().getSettings(new GetSettingsRequest().indices(index), future);
        return future;
    }

    public ActionFuture<UpdateSettingsResponse> updateSettings(Map<String, String> settings, String... indices) {
        logger.trace("Update settings with {} on {}", settings, Arrays.asList(indices));
        return client.admin().indices()
                .updateSettings(Requests.updateSettingsRequest(indices)
                        .settings(settings));
    }

    public ActionFuture<PutMappingResponse> putMapping(String mappingSource, String type, String... indices) {
        logger.trace("Put mapping with {} on {}", mappingSource, Arrays.asList(indices));
        return client.admin().indices()
                .putMapping(Requests.putMappingRequest(indices)
                        .source(mappingSource)
                        .type(type));
    }

    public ActionFuture<UpdateSettingsResponse> putSettings(String settingsSource, String index) {
        logger.trace("Put settings with {} on {}", settingsSource, index);
        return client.admin().indices()
                .updateSettings(Requests.updateSettingsRequest(index)
                        .settings(settingsSource));
    }

    public ActionFuture<IndicesAliasesResponse> switchAlias(String alias, String fromIndex, String toIndex) {
        logger.trace("Switch alias {} from {} to {}", alias, fromIndex, toIndex);
        return client.admin().indices().aliases(Requests.indexAliasesRequest()
                .removeAlias(fromIndex, alias)
                .addAlias(alias, toIndex));
    }
}
