package xyz.kenor.es;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

/**
 * @ClassName App
 * @Description elasticsearch
 * @Author JackChen
 * @Date 2020/1/16 10:01
 * @Version 1.0
 **/
public class App {
    TransportClient client = null;

    //获取客户段对象
    @SuppressWarnings({"resource", "unchecked"})
    @Before
    public void getClient() throws UnknownHostException {
        Settings settings = Settings.builder().put( "cluster.name", "my-application" ).build();
        //获取客户端连接对象
        client = new PreBuiltTransportClient( settings );
        client.addTransportAddress( new InetSocketTransportAddress( InetAddress.getByName( "datanode1" ), 9300 ) );
    }

    //创建索引
    @Test
    public void createIndex() {
        //创建索引
        client.admin().indices().prepareCreate( "log" ).get();
    }

    //删除索引
    @Test
    public void deleteIndex() {
        //删除索引
        client.admin().indices().prepareDelete( "log" ).get();
    }

    //创建文档以json形式
    @Test
    public void createIndexByJson() {
        //文档内容
        String json = "{" + "\"id\":\"1\"," + "\"title\":\"基于Lucene的检索服务\"," +
                "\"content\":\"它提供一个分布式多用户能力的全文搜索引擎，基于RESTFUL web接口\"" + "}";
        //创建
        IndexResponse response = client.prepareIndex( "blog", "article", "1" )
                .setSource( json )
                .execute()
                .actionGet();
        printMsg( response );
    }

    //创建文档以hashMap
    @Test
    public void createByHashMap() {
        HashMap<String, Object> json = new HashMap<>();
        json.put( "id", "2" );
        json.put( "title", "jackChen" );
        json.put( "content", "板砖" );

        IndexResponse response = client.prepareIndex( "blog", "article", "2" )
                .setSource( json )
                .execute()
                .actionGet();
        printMsg( response );
    }

    //创建文档以builder
    @Test
    public void createIndexByBuilder() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field( "id", "4" )
                .field( "title", "小说" )
                .field( "content", "百年孤独" )
                .endObject();
        IndexResponse response =
                client.prepareIndex( "blog", "article", "3" ).setSource( builder ).execute().actionGet();
        printMsg( response );
    }

    //单个索引查询
    @Test
    public void queryIndex() {
        GetResponse response = client.prepareGet( "blog", "article", "2" ).get();
        //打印
        System.out.println( response.getSourceAsString() );
    }

    //多个条件查询（批量查询）
    @Test
    public void queryMultiIndex() {
        MultiGetResponse responses = client.prepareMultiGet().add( "blog", "article", "3" )
                .add( "blog", "article", "2", "1" )
                .add( "blog", "article", "3" )
                .get();
        Iterator<MultiGetItemResponse> iterator = responses.iterator();
        while ( iterator.hasNext() ) {
            MultiGetItemResponse next = iterator.next();
            GetResponse response = next.getResponse();
            if ( response.isExists() ) {
                System.out.println( response.getSourceAsString() );
            }
        }
    }

    // 批量添加文档
    @Test
    public void bulkAddDoc() throws IOException {
        //批量请求
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.add( client.prepareIndex( "index", "fulltext", "5" )
                .setSource( XContentFactory.jsonBuilder()
                        .startObject()
                        .field( "content", "夜空中最亮的星" )
                        .endObject() )
        );
        bulkRequestBuilder.add( client.prepareIndex( "index", "fulltext", "6" )
                .setSource( XContentFactory.jsonBuilder()
                        .startObject()
                        .field( "content", "蒹葭苍苍，白露为霜" )
                        .endObject() )
        );
        BulkResponse bulkItemResponses = bulkRequestBuilder.get();
        System.out.println( bulkItemResponses.hasFailures() ? "添加失败" : "添加成功" );
    }

    //更新文档
    @Test
    public void updateDoc() throws IOException, ExecutionException, InterruptedException {
        UpdateRequest updateRequest = new UpdateRequest( "blog", "article", "2" );
        updateRequest.doc(
                XContentFactory.jsonBuilder().startObject()
                        .field( "id", "2" )
                        .field( "title", "github" )
                        .field( "content", "代码托管仓库" )
                        .endObject()
        );
        UpdateResponse updateResponse = client.update( updateRequest ).get();
        printMsg( updateResponse );
    }

    //更新文档 updateOrInsert，存在则更新，否则就创建
    @Test
    public void updateOrInsert() throws IOException, ExecutionException, InterruptedException {
        //没有就创建
        IndexRequest indexRequest = new IndexRequest( "blog", "article", "5" );
        indexRequest.source( XContentFactory.jsonBuilder().startObject()
                .field( "id", "3" )
                .field( "title", "小视频" )
                .field( "content", "抖音小视频" )
                .endObject() );
        //有文档内容就更新
        UpdateRequest updateRequest = new UpdateRequest( "blog", "article", "5" );
        updateRequest.doc(
                XContentFactory.jsonBuilder().startObject()
                        .field( "id", "2" )
                        .field( "title", "gitLib" )
                        .field( "content", "也是代码托管仓库" )
                        .endObject()
        );
        // 如果updateRequest 存在，则更新为updateRequest.doc所指内容，不存在就新建 indexRequest
        updateRequest.upsert( indexRequest );
        UpdateResponse updateResponse = client.update( updateRequest ).get();
        printMsg( updateResponse );
    }

    //删除文档
    @Test
    public void deleteDoc() {
        DeleteResponse deleteResponse = client.prepareDelete( "blog", "article", "5" ).get();
        printMsg( deleteResponse );
    }

    //查询所有文档
    @Test
    public void queryMatchAll() {
        SearchResponse searchResponse = client.prepareSearch( "blog" )
                .setTypes( "article" ).setQuery( QueryBuilders.matchAllQuery() )
                .get();
        SearchHits hits = searchResponse.getHits(); //命中的数量
        System.out.println( "查询结果为：" + hits.getTotalHits() );
        Iterator<SearchHit> iterator = hits.iterator();
        while ( iterator.hasNext() ) {
            SearchHit next = iterator.next();
            System.out.println( next.getSourceAsString() );
        }
    }

    //match query
    @Test
    public void matchQuery() {
        MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery( "id", "1" );
        SearchResponse response = client.prepareSearch( "blog" ).setTypes( "article" )
                .setQuery( queryBuilder ).get();
        SearchHits hits = response.getHits();
        System.out.println( "命中结果：" + hits.getTotalHits() );
        Iterator<SearchHit> iterator = hits.iterator();
        while ( iterator.hasNext() ) {
            SearchHit next = iterator.next();
            System.out.println( next.getSourceAsString() );
        }
    }

    // multi match query
    @Test
    public void multiMatchQuery() {
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery( "说", "title", "content" );
        SearchResponse response = client.prepareSearch( "blog" )
                .setTypes( "article" )
                .setQuery( multiMatchQueryBuilder ).get();
        SearchHits hits = response.getHits();
        System.out.println( "命中结果：" + hits.getTotalHits() );
        Iterator<SearchHit> iterator = hits.iterator();
        while ( iterator.hasNext() ) {
            SearchHit next = iterator.next();
            System.out.println( next.getSourceAsString() );
        }
    }

    //range query
    @Test
    public void rangeQuery() {
        RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery( "id" ).from( 1 ).to( 4 );
        SearchResponse response = client.prepareSearch( "blog" ).setQuery( queryBuilder ).get();
        SearchHits hits = response.getHits();
        System.out.println( "命中结果数量：" + hits.getTotalHits() );
        Iterator<SearchHit> iterator = hits.iterator();
        while ( iterator.hasNext() ) {
            SearchHit next = iterator.next();
            System.out.println( next.getSourceAsString() );
        }
    }

    // prefix query
    @Test
    public void prefixQuery(){
        PrefixQueryBuilder prefixQueryBuilder = QueryBuilders.prefixQuery( "title", "git" );
        SearchResponse response = client.prepareSearch( "blog" ).setQuery( prefixQueryBuilder ).get();
        SearchHits hits = response.getHits();
        System.out.println( "命中结果数量："+hits.getTotalHits() );
        Iterator<SearchHit> iterator = hits.iterator();
        while ( iterator.hasNext() ) {
            SearchHit next = iterator.next();
            System.out.println( next.getSourceAsString() );
        }
    }

    // 聚合查询
    @Test
    public void aggQuery() {
        AggregationBuilder aggregationBuilder = AggregationBuilders.max( "aggMax" ).field( "id" );
        SearchResponse response = client.prepareSearch( "log" ).addAggregation( aggregationBuilder ).get();
        Max maxId = response.getAggregations().get( "aggMax" );
        System.out.println( maxId.getValue() );
    }

    // 多条件查询
    @Test
    public void multiConditionQuery(){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must( QueryBuilders.matchQuery( "id", "4" ) )
                .mustNot( QueryBuilders.matchQuery( "title", "小说" ) );
        SearchResponse response = client.prepareSearch( "blog" ).setQuery( boolQueryBuilder ).get();
        SearchHits hits = response.getHits();
        Iterator<SearchHit> iterator = hits.iterator();
        while ( iterator.hasNext() ) {
            SearchHit next = iterator.next();
            System.out.println( next.getSourceAsString() );
        }
    }

    //桶聚合查询
    @Test
    public void filterAggQuery(){
        AggregationBuilder filters = AggregationBuilders.filters( "filters", new FiltersAggregator.KeyedFilter(
                "百年孤独", QueryBuilders.matchQuery(
                "content", "百年孤独" ) ) );
        SearchResponse response = client.prepareSearch( "blog" ).addAggregation( filters ).execute().actionGet();
        Filters aggregation = response.getAggregations().get( "filters" );
        for ( Filters.Bucket bucket : aggregation.getBuckets() ) {
            System.out.println( bucket.getKey() + ":" + bucket.getDocCount() );
        }
    }

        //字段分词查询
    @Test
    public void queryString() {
        SearchResponse response =
                client.prepareSearch( "blog" )
                        .setQuery( QueryBuilders.queryStringQuery( "小说" ) ).get();
        SearchHits hits = response.getHits();
        System.out.println( "查询结果为：" + hits.getTotalHits() );
        Iterator<SearchHit> iterator = hits.iterator();
        while ( iterator.hasNext() ) {
            SearchHit next = iterator.next();
            System.out.println( next.getSourceAsString() );
        }
    }

    //词条查询,默认mapping的分词器是standard 分词器，对中文支持单字符切分，如果要支持中文单词要使用ik分词器
    //这里index索引下的mapping 属性字段使用的是ik分词器
    @Test
    public void termQuery() {
        SearchResponse response = client.prepareSearch( "index" ).
                setQuery( QueryBuilders.termQuery( "content", "中国" ) )
                .get();
        SearchHits hits = response.getHits();
        System.out.println( "查询结果为：" + hits.getTotalHits() );
        Iterator<SearchHit> iterator = hits.iterator();
        while ( iterator.hasNext() ) {
            SearchHit next = iterator.next();
            System.out.println( next.getSourceAsString() );
        }
    }

    //通配符查询
    @Test
    public void wildQuery() {
        SearchResponse response = client.prepareSearch( "blog" )
                //* 表示任意个字符
                //？ 表示单个字符
                .setQuery( QueryBuilders.wildcardQuery( "content", "*擎*" ) )
                .get();
        SearchHits hits = response.getHits();
        System.out.println( "查询结果：" + hits.getTotalHits() );
        Iterator<SearchHit> iterator = hits.iterator();
        while ( iterator.hasNext() ) {
            SearchHit next = iterator.next();
            System.out.println( next.getSourceAsString() );
        }
    }

    //模糊查询
    @Test
    public void fuzzyQuery() {
        SearchResponse response = client.prepareSearch( "blog" )
                .setQuery( QueryBuilders.fuzzyQuery( "title", "github" ) ).get();
        SearchHits hits = response.getHits();
        System.out.println( "查询结果为：" + hits.getTotalHits() );
        Iterator<SearchHit> iterator = hits.iterator();
        while ( iterator.hasNext() ) {
            SearchHit next = iterator.next();
            System.out.println( next.getSourceAsString() );
        }
    }

    //创建mapping,事先必须先建立索引
    @Test
    public void createMapping() throws IOException, ExecutionException, InterruptedException {
        //内部为json结构
        /**
         * {
         * 	"mappings": {
         * 		"article": {
         * 			"properties": {
         * 				"id": {
         * 					"type": "text",
         * 					"fields": {
         * 						"keyword": {
         * 							"ignore_above": 256,
         * 							"type": "keyword"
         *                                  }
         *                             }
         *                     }
         *                      }
         *                  }
         *              }
         * }
         */
        XContentBuilder xContentBuilder =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject( "article" )
                        .startObject( "properties" )
                        .startObject( "id" )
                        .field( "type", "long" )
                        .field( "store", "yes" )
                        .endObject()
                        .startObject( "title" )
                        .field( "type", "string" )
                        .field( "store", "yes" )
                        .endObject()
                        .startObject( "content" )
                        .field( "type", "string" )
                        .field( "store", "yes" )
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject();
        PutMappingRequest mapping = Requests.putMappingRequest( "log" ).type( "article" ).source( xContentBuilder );
        client.admin().indices().putMapping( mapping ).get();
    }

    public void printMsg(DocWriteResponse response) {
        //打印返回值
        System.out.println( "索引：" + response.getIndex() );
        System.out.println( "类型：" + response.getType() );
        System.out.println( "id：" + response.getId() );
        System.out.println( "版本：" + response.getVersion() );
        System.out.println( "结果：" + response.getResult() );
    }

    //关闭资源
    @After
    public void closeClient() {
        client.close();
    }
}
