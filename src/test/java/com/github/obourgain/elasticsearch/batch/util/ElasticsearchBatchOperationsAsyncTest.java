package com.github.obourgain.elasticsearch.batch.util;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Before;
import org.junit.Test;

public class ElasticsearchBatchOperationsAsyncTest extends ElasticsearchIntegrationTest {

    public static final String INDEX = "index";
    public static final String INDEX_2 = "index2";
    public static final String ALIAS = "alias";
    public static final String TYPE = "type";
    private ElasticsearchBatchOperationsAsync operations;

    @Before
    public void setup() {
        createIndex(INDEX);
        operations = new ElasticsearchBatchOperationsAsync(client());
    }

    @Test
    public void testDisableRefresh() {
        operations.disableRefresh(INDEX).actionGet();
        String refreshInterval = operations.getRefreshInterval(INDEX).actionGet();
        Assertions.assertThat(refreshInterval).isEqualTo("-1");
    }

    @Test
    public void testRefresh() throws ExecutionException, InterruptedException {
        operations.disableRefresh(INDEX).actionGet();
        indexRandom(false, true, new IndexRequestBuilder(client()).setIndex(INDEX).setType(TYPE).setId("id").setSource("foo", "bar"));

        GetResponse doc = client().get(Requests.getRequest(INDEX).type(TYPE).id("id")).actionGet();
        Assertions.assertThat(doc.isExists()).isTrue();

        SearchResponse searchResponse = client().search(Requests.searchRequest(INDEX).source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))).actionGet();
        ElasticsearchAssertions.assertHitCount(searchResponse, 0);

        operations.refresh(INDEX).actionGet();

        SearchResponse searchResponseAfterRefresh = client().search(Requests.searchRequest(INDEX).source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))).actionGet();
        ElasticsearchAssertions.assertHitCount(searchResponseAfterRefresh, 1);
    }

    @Test
    public void testSetRefreshInterval() {
        operations.setRefreshInterval("10s", INDEX).actionGet();

        String refreshInterval = operations.getRefreshInterval(INDEX).actionGet();

        Assertions.assertThat(refreshInterval).isEqualTo("10s");
    }

    @Test
    public void testUpdateSettings() {
        // just picked a random one that is null
        String setting = getSettings().getSetting(INDEX, "index.fail_on_merge_failure");
        Assertions.assertThat(setting).isNull();

        operations.updateSettings(Collections.singletonMap("index.fail_on_merge_failure", "false"), INDEX).actionGet();

        String settingAfterModification = getSettings().getSetting(INDEX, "index.fail_on_merge_failure");
        Assertions.assertThat(settingAfterModification).isEqualTo("false");
    }

    @Test
    public void testDisableReplicas() throws ExecutionException, InterruptedException {
        indexRandom(false, true, new IndexRequestBuilder(client()).setIndex(INDEX).setType(TYPE).setId("id").setSource("foo", "bar"));
        String setting = getSettings().getSetting(INDEX, "index.number_of_replicas");
        Assertions.assertThat(setting).isEqualTo("1");

        operations.disableReplicas(INDEX).actionGet();

        String numberOfReplicasAfter = getSettings().getSetting(INDEX, "index.number_of_replicas");
        Assertions.assertThat(numberOfReplicasAfter).isEqualTo("0");
    }

    @Test
    public void testSetReplicas() {
        String setting = getSettings().getSetting(INDEX, "index.number_of_replicas");
        Assertions.assertThat(setting).isEqualTo("1");

        operations.setReplicas(3, INDEX).actionGet();

        String numberOfReplicasAfter = getSettings().getSetting(INDEX, "index.number_of_replicas");
        Assertions.assertThat(numberOfReplicasAfter).isEqualTo("3");
    }

    @Test
    public void testSwitchAlias() {
        createIndex(INDEX_2);
        client().admin().indices().aliases(Requests.indexAliasesRequest().addAlias(ALIAS, INDEX)).actionGet();

        operations.switchAlias(ALIAS, INDEX, INDEX_2).actionGet();

        GetAliasesResponse response = client().admin().indices().getAliases(new GetAliasesRequest(ALIAS)).actionGet();
        Assertions.assertThat(response.getAliases().size()).isEqualTo(1);
        ObjectObjectCursor<String, List<AliasMetaData>> cursor = response.getAliases().iterator().next();
        Assertions.assertThat(cursor.key).isEqualTo(INDEX_2);
        Assertions.assertThat(cursor.value.size()).isEqualTo(1);
        Assertions.assertThat(cursor.value.iterator().next().alias()).isEqualTo(ALIAS);
    }

    private GetSettingsResponse getSettings() {
        return client().admin().indices().getSettings(new GetSettingsRequest().indices(INDEX)).actionGet();
    }

    @Override
    protected int numberOfReplicas() {
        // this may cause the index to have 0 replicas, and that breaks the assertions for tests that change replica count
        return 1;
    }
}