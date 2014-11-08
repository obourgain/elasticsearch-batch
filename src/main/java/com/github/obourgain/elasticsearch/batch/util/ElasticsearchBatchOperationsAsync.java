package com.github.obourgain.elasticsearch.batch.util;

import java.util.Collections;
import java.util.Map;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.support.AdapterActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;

public class ElasticsearchBatchOperationsAsync {

    private final Client client;

    public ElasticsearchBatchOperationsAsync(Client client) {
        this.client = client;
    }

    public ActionFuture<RefreshResponse> refresh(String... indices) {
        return client.admin().indices().refresh(Requests.refreshRequest(indices));
    }

    public ActionFuture<UpdateSettingsResponse> disableRefresh(String... indices) {
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
        return updateSettings(Collections.singletonMap("refresh_interval", refreshInterval), indices);
    }

    public ActionFuture<UpdateSettingsResponse> disableReplicas(String... indices) {
        return updateSettings(Collections.singletonMap("number_of_replicas", "0"), indices);
    }

    public ActionFuture<UpdateSettingsResponse> setReplicas(int replicas, String... indices) {
        return updateSettings(Collections.singletonMap("number_of_replicas", String.valueOf(replicas)), indices);
    }

    public ActionFuture<UpdateSettingsResponse> updateSettings(Map<String, String> settings, String... indices) {
        return client.admin().indices()
                .updateSettings(Requests.updateSettingsRequest(indices)
                        .settings(settings));
    }

    public ActionFuture<IndicesAliasesResponse> switchAlias(String alias, String fromIndex, String toIndex) {
        return client.admin().indices().aliases(Requests.indexAliasesRequest()
                .removeAlias(fromIndex, alias)
                .addAlias(alias, toIndex));
    }

}
