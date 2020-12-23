package com.kungeek.util;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.cluster.Health;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

public class EsUtil {
    static JestClientFactory factory = null;

    static protected final Log log = LogFactory.getLog(EsUtil.class.getName());

    public static JestClient getClient(){

        if(factory == null){
            build();
        }
        return factory.getObject();
    }

    private static void build(){

        if(factory == null){

            factory = new JestClientFactory();
            factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hxduat.kungeek.com/es/" )
                    .defaultCredentials("elastic", "123456")
                    .multiThreaded(true)
                    .maxTotalConnection(20)
                    .connTimeout(10000).readTimeout(1000).build());

        }
    }

    public static void putIndex(String[] args) throws IOException {
        JestClient client = getClient();

        Users users = new Users("01", "zhangyubo", "123456");

        Index index = new Index.Builder(users).index("es_test").type("_doc").id(users.getId()).build();

        client.execute(index);

        client.close();
    }

    public void search() throws IOException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        QueryBuilder queryBuilder = QueryBuilders.termQuery("staff_name", "郭飒飞");

        searchSourceBuilder.query(queryBuilder);

        searchSourceBuilder.from(0);
        searchSourceBuilder.size(20);
        String query = searchSourceBuilder.toString();
        System.out.println(query);

        String query01 = "{\n" +
                "  \"from\" : 0,\n" +
                "  \"size\" : 20,\n" +
                "  \"query\" : {\n" +
                "    \"term\" : {\n" +
                "      \"staff_name\" : \"郭飒飞\"\n" +
                "    }\n" +
                "  }\n" +
                "}";

        Search search = new Search.Builder(query01).addIndex("staff_tag").addType("_doc").build();
        JestClient client = getClient();
        SearchResult result = client.execute(search);

        List<SearchResult.Hit<Object, Void>> hits = result.getHits(Object.class);

        System.out.println("Size" + hits.size());

        for (SearchResult.Hit<Object, Void> hit : hits){
            System.out.println(hit.source.toString());
        }

        client.close();
    }

    public static JestResult health(){
        Health health = new Health.Builder().build();
        JestResult result = null;
        try {
            result = getClient().execute(health);
            log.info("health == " + result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }


    @Test
    public void test() throws IOException {

    }
}
