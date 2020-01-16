package xyz.kenor.es;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
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
        client.admin().indices().prepareCreate( "blog" ).get();
    }

    //删除索引
    @Test
    public void deleteIndex() {
        //删除索引
        client.admin().indices().prepareDelete( "blog" ).get();
    }

    //创建文档以json形式
    @Test
    public void createIndexByJson() {
        //文档内容
        String json = "{" + "\"id\":\"1\"," + "\"title\":\"基于Lucene的检索服务\"," +
                "\"content\":\"它提供一个分布式多用户能力的全文搜索引擎，基于RESTFUL web接口\"" + "}";
        //创建
        IndexResponse response = client.prepareIndex( "blog", "article", "1" ).setSource( json ).execute().actionGet();
        printMsg( response );
    }

    //创建文档以hashMap
    @Test
    public void createByHashMap() {
        HashMap<String, Object> json = new HashMap<>();
        json.put( "id", "2" );
        json.put( "title", "jackChen" );
        json.put( "content", "板砖" );

        IndexResponse response = client.prepareIndex( "blog", "article", "2" ).setSource( json ).execute().actionGet();
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

    //多个条件查询
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
        SearchResponse searchResponse = client.prepareSearch( "blog" ).setTypes( "article" ).setQuery( QueryBuilders.matchAllQuery() ).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println( "查询结果为：" + hits.getTotalHits() );
        Iterator<SearchHit> iterator = hits.iterator();
        while ( iterator.hasNext() ) {
            SearchHit next = iterator.next();
            System.out.println( next.getSourceAsString() );
        }
    }

    //字段分词查询
    @Test
    public void queryString(){
        SearchResponse response =
                client.prepareSearch( "blog" ).setQuery( QueryBuilders.queryStringQuery( "小说" ) ).get();
        SearchHits hits = response.getHits();
        System.out.println( "查询结果为："+hits.getTotalHits() );
        Iterator<SearchHit> iterator = hits.iterator();
        while ( iterator.hasNext() ) {
            SearchHit next = iterator.next();
            System.out.println( next.getSourceAsString() );
        }
    }

    //词条查询
    @Test
    public void termQuery(){
        SearchResponse response = client.prepareSearch( "blog" ).
                setQuery( QueryBuilders.termQuery( "content", "百" ) )
                .get();
        SearchHits hits = response.getHits();
        System.out.println("查询结果为："+hits.getTotalHits());
        Iterator<SearchHit> iterator = hits.iterator();
        while ( iterator.hasNext() ) {
            SearchHit next = iterator.next();
            System.out.println(next.getSourceAsString());
        }

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
