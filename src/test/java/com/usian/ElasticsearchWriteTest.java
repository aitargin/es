package com.usian;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.io.IOException;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class ElasticsearchWriteTest {

    @Resource
    private RestHighLevelClient restHighLevelClient;

    //创建索引库
    @Test
    public void testCreateIndex() throws IOException {
        //创建索引操作客户端
        IndicesClient indices = restHighLevelClient.indices();
        //创建“创建索引请求”对象，并设置索引名称
        CreateIndexRequest request = new CreateIndexRequest("java2002");
        request.settings("{\n" +
                "\"number_of_shards\": 3,\n" +
                "\"number_of_replicas\": 1\n" +
                "}", XContentType.JSON);
        request.mapping("argin","{\n" +
                "\"_source\": {\n" +
                "\"excludes\":[\"description\"]\n" +
                "}, \n" +
                "\t\"properties\": {\n" +
                "\"name\": {\n" +
                "\"type\": \"text\",\n" +
                "\"analyzer\":\"ik_max_word\",\n" +
                "\"search_analyzer\":\"ik_smart\"\n" +
                "},\n" +
                "\"description\": {\n" +
                "\"type\": \"text\",\n" +
                "\"analyzer\":\"ik_max_word\",\n" +
                "\"search_analyzer\":\"ik_smart\"\n" +
                "},\n" +
                "\"studymodel\": {\n" +
                "\"type\": \"keyword\"\n" +
                "},\n" +
                "\"price\": {\n" +
                "\"type\": \"float\"\n" +
                "},\n" +
                "\"pic\":{\n" +
                "\t\t\"type\":\"text\",\n" +
                "\t\t\"index\":false\n" +
                "\t},\n" +
                "\"timestamp\": {\n" +
                "\t\t\"type\":   \"date\",\n" +
                "\t\t\"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd\"\n" +
                "\t }\n" +
                "}\n" +
                "}",XContentType.JSON);
        //创建响应对象
        CreateIndexResponse createIndexResponse = indices.create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse.isAcknowledged());
    }

    //删除索引库
    @Test
    public void testDeleteIndex() throws IOException {
        //创建“删除索引请求”对象
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("java2002");
        //创建索引操作客户端
        IndicesClient indices = restHighLevelClient.indices();
        //创建响应对象
        DeleteIndexResponse deleteIndexResponse = indices.delete(deleteIndexRequest,RequestOptions.DEFAULT);
        //得到响应结果
        boolean acknowledged = deleteIndexResponse.isAcknowledged();
        System.out.println(acknowledged);
    }

    //添加文档
    @Test
    public void testAddDocument() throws IOException {
        //创建“索引请求”对象：索引当动词
        IndexRequest request = new IndexRequest("java2002","argin","1");
        request.source("{\n" +
                " \"name\":\"spring cloud实战\",\n" +
                " \"description\":\"本课程主要从四个章节进行讲解： 1.微服务架构入门 2.spring cloud 基础入门 3.实战Spring Boot 4.注册中心eureka。\",\n" +
                " \"studymodel\":\"201001\",\n" +
                " \"price\":5.6\n" +
                "}",XContentType.JSON);
        IndexResponse index = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        System.out.println(index.toString());
    }

    //批量添加文档
    @Test
    public void testBulkAddDocument() throws IOException {
        //创建批量添加对象
        BulkRequest bulkRequest = new BulkRequest();
        //批量添加
        bulkRequest.add(new IndexRequest("java2002","argin").source("{\"name\":\"php实战\",\"description\":\"php谁都不服\",\"studymodel\":\"201001\",\"price\":\"5.6\"}",XContentType.JSON));
        bulkRequest.add(new IndexRequest("java2002","argin").source("{\"name\":\"net实战\",\"description\":\"net从入门到放弃\",\"studymodel\":\"201001\",\"price\":\"7.6\"}",XContentType.JSON));
        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.hasFailures());

    }

    //更新文档
    @Test
    public void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("java2002","argin","1");
        updateRequest.doc("{\n" +
                " \"price\":6.66\n" +
                "}",XContentType.JSON);
        UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        DocWriteResponse.Result result = update.getResult();
        System.out.println(result);
    }

    //根据id删除文档
    @Test
    public void testDelDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("java2002", "argin", "1");
        DeleteResponse delete = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.getResult());
    }
}
