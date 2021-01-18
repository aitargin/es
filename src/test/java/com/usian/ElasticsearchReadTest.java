package com.usian;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class ElasticsearchReadTest {
    @Resource
    private RestHighLevelClient restHighLevelClient;

    private SearchRequest request;
    private SearchResponse response;

    @Before
    public void initSearchRequest() {
        // 搜索请求对象
        request = new SearchRequest("java2002");//代表mysql里的database
        request.types("argin");//代表MySQL里的表
    }

    //遍历
    @After
    public void displayDoc() {
        // 搜索匹配结果
        SearchHits hits = response.getHits();
        // 搜索总记录数
        long totalHits = hits.totalHits;
        System.out.println("共搜索到" + totalHits + "条文档");
        // 匹配的文档
        SearchHit[] searchHits = hits.getHits();
        // 日期格式化对象
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (SearchHit hit : searchHits) {
            // 文档id
            String id = hit.getId();
            System.out.println("id：" + id);
            // 源文档内容
            String source = hit.getSourceAsString();
            System.out.println(source);
            Map<String, HighlightField> map = hit.getHighlightFields();
            if (map != null && map.size() != 0) {
                HighlightField field = map.get("name");
                Text[] fragments = field.getFragments();
                System.out.println("高亮字段：" + fragments[0].toString());
            }
        }
    }

    //简单查询文档
    @Test
    public void getDoc() throws IOException {
        GetRequest request = new GetRequest("java2002", "argin", "1");
        GetResponse documentFields = restHighLevelClient.get(request);
        System.out.println(documentFields.getSourceAsString());
    }

    // 搜索type下的全部记录
    @Test
    public void testSearchAll() throws Exception {
        // 搜索源构建对象 也就是搜索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        // 设置搜索源
        request.source(builder);
        response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
    }

    //分页查询
    @Test
    public void testSearchPage() throws Exception {
        //搜索构建对象 搜索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.from(0);
        builder.size(2);
        //设置搜索源
        request.source(builder);
        //执行搜索
        response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
    }

    //关键字匹配查询
    @Test
    public void testMatchQuery() throws Exception {
        //构建搜索条件对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name", "spring开发"));
        //不加就是spring 开发 或 spring开发 加了就是spring开发
        //searchSourceBuilder.query(QueryBuilders.matchQuery("name","spring开发").operator(Operator.AND));
        //设置搜索源
        request.source(searchSourceBuilder);
        //执行搜索
        response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
    }

    //多关键字匹配查询
    @Test
    public void testMultiMatchQuery() throws Exception {
        //构建搜索条件对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("net", "name", "description"));
        //设置搜索源
        request.source(searchSourceBuilder);
        //执行搜索
        response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
    }

    //bool（布尔）查询组合查询，区间查
    @Test
    public void testBooleanMatch() throws IOException {
        //构建搜索条件对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //json条件
        BoolQueryBuilder builder = QueryBuilders.boolQuery();
        builder.must(QueryBuilders.matchQuery("name", "开发"));
        builder.must(QueryBuilders.rangeQuery("price").gte(1).lte(100));
        searchSourceBuilder.query(builder);
        request.source(searchSourceBuilder);
        response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
    }

    //filter查询对布尔查询的优化
    @Test
    public void testFilterQuery() throws IOException {
        //构建搜索条件对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder builder = QueryBuilders.boolQuery();
        builder.must(QueryBuilders.matchQuery("name", "net"));
        builder.filter(QueryBuilders.rangeQuery("price").gte(1).lte(100));
        searchSourceBuilder.query(builder);
        request.source(searchSourceBuilder);
        response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
    }

    //highlight查询(高亮显示)
    @Test
    public void testHighLightQuery() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name", "开发"));
        //设置高亮
        HighlightBuilder builder = new HighlightBuilder();
        builder.preTags("<font color='red'>");//前缀
        builder.postTags("</font>");//后缀
        builder.field("name");//一个field变
        //builder.fields().add(new HighlightBuilder.Field("name"));也可以多个 add是一个，没有add的是field集合

        searchSourceBuilder.highlighter(builder);

        request.source(searchSourceBuilder);
        response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
    }

}
