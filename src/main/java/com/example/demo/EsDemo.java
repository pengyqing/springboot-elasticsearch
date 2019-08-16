package com.example.demo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/demo")
public class EsDemo
{
    @Autowired
    private RestHighLevelClient highLevelClient;
    
    /**
     * 创建索引
     */
    @SuppressWarnings("deprecation")
    @GetMapping("createIndex")
    public void createIndex()
    {
        try
        {
            Map<String, Object> properties = new HashMap<String, Object>();
            Map<String, Object> propertie = new HashMap<String, Object>();
            propertie.put("type", "text"); // 类型
            // propertie.put("index",true);
            propertie.put("analyzer", "ik_max_word"); // 分词器
            properties.put("title", propertie);
            
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject()
                .startObject("mappings")
                .startObject("_doc")
                .field("properties", properties)
                .endObject()
                .endObject()
                .startObject("settings")
                .field("number_of_shards", 3)
                .field("number_of_replicas", 1)
                .endObject()
                .endObject();
            
            CreateIndexRequest request = new CreateIndexRequest("demo").source(builder);
            highLevelClient.indices().create(request, RequestOptions.DEFAULT);
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    /**
     * 插入文档
     */
    @GetMapping("update")
    public void update()
    {
        try
        {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("title", "我是一条测试文档数据");
            builder.endObject();
            
            IndexRequest request = new IndexRequest("demo").source(builder);
            highLevelClient.index(request, RequestOptions.DEFAULT);
            
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    /**
     * 查询文档
     */
    @GetMapping("search")
    public void documentPage(String title)
    {
        Integer pageIndex = 1;
        Integer pageSize = 5;
        String indexName = "demo";
        Map<String, Object> data = new HashMap<>();
        data.put("title", title); 
        
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        SearchRequest searchRequest = new SearchRequest(indexName);
        // searchRequest.types(indexName);
        queryBuilder(pageIndex, pageSize, data, indexName, searchRequest);
        try
        {
            SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            for (SearchHit hit : response.getHits().getHits())
            {
                Map<String, Object> map = hit.getSourceAsMap();
                map.put("id", hit.getId());
                result.add(map);
                
                // 取高亮结果
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                HighlightField highlight = highlightFields.get("title");
                Text[] fragments = highlight.fragments(); // 多值的字段会有多个值
                String fragmentString = fragments[0].string();
                System.out.println("高亮：" + fragmentString);
            }
            System.out.println("pageIndex:" + pageIndex);
            System.out.println("pageSize:" + pageSize);
            System.out.println(response.getHits().getTotalHits());
            System.out.println(result.size());
            for (Map<String, Object> map : result)
            {
                System.out.println(map.get("title"));
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
    
    private void queryBuilder(Integer pageIndex, Integer pageSize, Map<String, Object> query, String indexName,
        SearchRequest searchRequest)
    {
        if (query != null && !query.keySet().isEmpty())
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            if (pageIndex != null && pageSize != null)
            {
                searchSourceBuilder.size(pageSize);
                if (pageIndex <= 0)
                {
                    pageIndex = 0;
                }
                searchSourceBuilder.from((pageIndex - 1) * pageSize);
            }
            BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
            query.keySet().forEach(key -> {
                boolBuilder.must(QueryBuilders.matchQuery(key, query.get(key)));
                
            });
            searchSourceBuilder.query(boolBuilder);
            
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            HighlightBuilder.Field highlightTitle =
                new HighlightBuilder.Field("title").preTags("<strong>").postTags("</strong>");
            highlightTitle.highlighterType("unified");
            highlightBuilder.field(highlightTitle);
            searchSourceBuilder.highlighter(highlightBuilder);
            
            searchRequest.source(searchSourceBuilder);
        }
    }
    
}
