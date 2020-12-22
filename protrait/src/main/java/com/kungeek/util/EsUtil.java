package com.kungeek.util;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class EsUtil {
    static JestClientFactory factory = null;

    public static JestClient getClient(){

        if(factory == null){
            build();
        }
        return factory.getObject();
    }

    public static void build(){

        if(factory == null){

            factory = new JestClientFactory();
            factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hxduat.kungeek.com/es/" )
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

    public static String search(String s) throws IOException {

        SearchSourceBuilder sebu = new SearchSourceBuilder();
        BoolQueryBuilder bo = new BoolQueryBuilder();

        bo.should(new MatchQueryBuilder("", ""));

        sebu.query(bo);

        sebu.from(0);
        sebu.size(20);
        sebu.highlight(new HighlightBuilder().field(""));

        String query2 = sebu.toString();
        System.out.println(query2);

        Search search = new Search.Builder(query2).addIndex("").addType("").build();

        JestClient client = getClient();
        SearchResult result = client.execute(search);

        System.out.println(result);

        client.close();

        return null;
    }


    @Test
    public void test() throws IOException {
        getClient();
        search("");
    }
}
