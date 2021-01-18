package com.usian;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.io.IOException;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class EsTestApplicationTests {

    @Resource
    private RestHighLevelClient restHighLevelClient;


    //创建索引库
    @Test
    public void testCreateIndex() throws IOException {
        //创建“创建索引请求”对象，并设置索引名称
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("java1906");
        //设置索引参数
        createIndexRequest.settings(Settings.builder().put("number_of_shards",1).
                put("number_of_replicas",0));
        createIndexRequest.mapping("course", "{\r\n" +
                "  \"_source\": {\r\n" +
                "    \"excludes\":[\"description\"]\r\n" +
                "  }, \r\n" +
                " 	\"properties\": {\r\n" +
                "           \"name\": {\r\n" +
                "              \"type\": \"text\",\r\n" +
                "              \"analyzer\":\"ik_max_word\",\r\n" +
                "              \"search_analyzer\":\"ik_smart\"\r\n" +
                "           },\r\n" +
                "           \"description\": {\r\n" +
                "              \"type\": \"text\",\r\n" +
                "              \"analyzer\":\"ik_max_word\",\r\n" +
                "              \"search_analyzer\":\"ik_smart\"\r\n" +
                "           },\r\n" +
                "           \"studymodel\": {\r\n" +
                "              \"type\": \"keyword\"\r\n" +
                "           },\r\n" +
                "           \"price\": {\r\n" +
                "              \"type\": \"float\"\r\n" +
                "           },\r\n" +
                "           \"timestamp\": {\r\n" +
                "          		\"type\":   \"date\",\r\n" +
                "          		\"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd\"\r\n" +
                "        	}\r\n" +
                "  }\r\n" +
                "}", XContentType.JSON);
        //创建索引操作客户端
        IndicesClient indices = restHighLevelClient.indices();

        //创建响应对象
        CreateIndexResponse createIndexResponse =
                indices.create(createIndexRequest, RequestOptions.DEFAULT);
        //得到响应结果
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println(acknowledged);
    }

    //删除索引库
    @Test
    public void testDeleteIndex() throws IOException {
        //创建“删除索引请求”对象
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("java1906");
        //创建索引操作客户端
        IndicesClient indices = restHighLevelClient.indices();
        //创建响应对象
        DeleteIndexResponse deleteIndexResponse =
                indices.delete(deleteIndexRequest,RequestOptions.DEFAULT);
        //得到响应结果
        boolean acknowledged = deleteIndexResponse.isAcknowledged();
        System.out.println(acknowledged);
    }

}
