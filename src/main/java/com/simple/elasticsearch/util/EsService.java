package com.simple.elasticsearch.util;

import com.alibaba.fastjson.JSON;
import com.simple.elasticsearch.annotation.EsId;
import com.simple.elasticsearch.function.GFunction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.annotations.Document;

import java.io.*;
import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zcw
 * @version 1.0
 * @date 2021/1/14 10:47
 */
public abstract class EsService<T> implements Serializable {

    @Autowired
    private RestHighLevelClient client;

    private String index;

    private Class<T> c;

    private Method getId;

    private static final String get = "get";

    protected EsService() {
        Type type = this.getClass().getGenericSuperclass();
        ParameterizedType parameterizedType = (ParameterizedType) type;
        Type[] t = parameterizedType.getActualTypeArguments();
        this.c = (Class<T>) t[0];
        if (this.c.isAnnotationPresent(Document.class)) {
            this.index = this.c.getAnnotation(Document.class).indexName();
        }
        Field[] fields = this.c.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(EsId.class)) {
                String fieldName = field.getName();
                String FieldName = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
                String methodName = get + FieldName;
                Method getMethod = null;
                try {
                    getMethod = this.c.getMethod(methodName, null);
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                }
                this.getId = getMethod;
            }
        }
    }

    public boolean indexExist() throws Exception {
        GetIndexRequest request = new GetIndexRequest(index);
        request.local(false);
        request.humanReadable(true);
        request.includeDefaults(false);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    public void insertOrUpdateOne(T entity) {
        IndexRequest request = new IndexRequest(index);
        try {
            request.id(this.getId.invoke(entity, null).toString());
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        request.source(JSON.toJSONString(entity), XContentType.JSON);
        try {
            client.index(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void insertBatch(List<T> list) {
        BulkRequest request = new BulkRequest();
        list.forEach(item -> {
            try {
                request.add(new IndexRequest(index).id(getId.invoke(item, null).toString())
                        .source(JSON.toJSONString(item), XContentType.JSON));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        });
        try {
            client.bulk(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteBatch(List<Long> idList) {
        BulkRequest request = new BulkRequest();
        idList.forEach(item -> request.add(new DeleteRequest(index, item.toString())));
        try {
            client.bulk(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<T> search(SearchSourceBuilder builder) {
        SearchRequest request = new SearchRequest(index);
        request.source(builder);
        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            SearchHit[] hits = response.getHits().getHits();
            List<T> res = new ArrayList<>(hits.length);
            for (SearchHit hit : hits) {
                res.add(JSON.parseObject(hit.getSourceAsString(), c));
            }
            return res;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteIndex() {
        try {
            client.indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteByQuery(QueryBuilder builder) {
        DeleteByQueryRequest request = new DeleteByQueryRequest(index);
        request.setQuery(builder);
        request.setBatchSize(10000);
        request.setConflicts("proceed");
        try {
            client.deleteByQuery(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected EsQueryBuilder esLambdaQuery() {
        return new EsQueryBuilder();
    }

    public class EsQueryBuilder {

        private SearchSourceBuilder searchSourceBuilder;

        private EsQueryBuilder() {
            if (this.searchSourceBuilder == null) {
                searchSourceBuilder = new SearchSourceBuilder();
            }
        }

        public EsQueryBuilder eq(GFunction<? extends T, Object> gFunction, Object value) {
            this.searchSourceBuilder.postFilter(QueryBuilders.termQuery(gFunction.field(), value));
            return this;
        }

        public EsQueryBuilder notEq(GFunction<? extends T, Object> gFunction, Object value) {
            this.searchSourceBuilder.postFilter(new BoolQueryBuilder().mustNot(QueryBuilders.termQuery(gFunction.field(), value)));
            return this;
        }

        public EsQueryBuilder in(GFunction<? extends T, Object> gFunction, Object... values) {
            String fieldName = gFunction.field();
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            for (Object value : values) {
                boolQueryBuilder.should(QueryBuilders.termQuery(fieldName, value));
            }
            this.searchSourceBuilder.postFilter(boolQueryBuilder);
            return this;
        }

        public EsQueryBuilder notIn(GFunction<? extends T, Object> gFunction, Object... values) {
            String fieldName = gFunction.field();
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            for (Object value : values) {
                boolQueryBuilder.mustNot(QueryBuilders.termQuery(fieldName, value));
            }
            this.searchSourceBuilder.postFilter(boolQueryBuilder);
            return this;
        }

        public EsQueryBuilder between(GFunction<? extends T, Object> gFunction, Object begin, Object end) {
            this.searchSourceBuilder.postFilter(new BoolQueryBuilder().filter(QueryBuilders.rangeQuery(gFunction.field()).from(begin).to(end)));
            return this;
        }

        public EsQueryBuilder gt(GFunction<? extends T, Object> gFunction, Object value) {
            this.searchSourceBuilder.postFilter(QueryBuilders.rangeQuery(gFunction.field()).gt(value));
            return this;
        }

        public EsQueryBuilder lt(GFunction<? extends T, Object> gFunction, Object value) {
            this.searchSourceBuilder.postFilter(QueryBuilders.rangeQuery(gFunction.field()).lt(value));
            return this;
        }

        public EsQueryBuilder ge(GFunction<? extends T, Object> gFunction, Object value) {
            this.searchSourceBuilder.postFilter(QueryBuilders.rangeQuery(gFunction.field()).gte(value));
            return this;
        }

        public EsQueryBuilder le(GFunction<? extends T, Object> gFunction, Object value) {
            this.searchSourceBuilder.postFilter(QueryBuilders.rangeQuery(gFunction.field()).lte(value));
            return this;
        }

        public EsQueryBuilder sort(GFunction<? extends T, Object> gFunction, SortOrder value) {
            this.searchSourceBuilder.sort(gFunction.field(), value);
            return this;
        }

        public EsQueryBuilder page(Integer pageNo, Integer pageSize) {
            pageNo = (pageNo - 1) * pageSize;
            this.searchSourceBuilder.from(pageNo).size(pageSize);
            return this;
        }

        public EsQueryBuilder matchAll(GFunction<? extends T, Object> gFunction, Object... values) {
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            String fieldName = gFunction.field();
            for (Object value : values) {
                boolQueryBuilder.filter(QueryBuilders.matchPhrasePrefixQuery(fieldName, value));
            }
            this.searchSourceBuilder.postFilter(boolQueryBuilder);
            return this;
        }

        public EsQueryBuilder matchOne(GFunction<? extends T, Object> gFunction, Object... values) {
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            String fieldName = gFunction.field();
            for (Object value : values) {
                boolQueryBuilder.should(QueryBuilders.matchPhrasePrefixQuery(fieldName, value));
            }
            this.searchSourceBuilder.postFilter(boolQueryBuilder);
            return this;
        }

        public EsQueryBuilder fuzzyAll(GFunction<? extends T, Object> gFunction, Fuzziness fuzziness, Object... values) {
            String fieldName = gFunction.field();
            for (Object value : values) {
                this.searchSourceBuilder.postFilter(QueryBuilders.fuzzyQuery(fieldName, value).fuzziness(fuzziness));
            }
            return this;
        }

        public EsQueryBuilder fuzzyOne(GFunction<? extends T, Object> gFunction, Fuzziness fuzziness, Object... values) {
            String fieldName = gFunction.field();
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            for (Object value : values) {
                boolQueryBuilder.should(QueryBuilders.fuzzyQuery(fieldName, value).fuzziness(fuzziness));
            }
            this.searchSourceBuilder.postFilter(boolQueryBuilder);
            return this;
        }

        public SearchSourceBuilder end() {
            return this.searchSourceBuilder;
        }

        public List<T> query(){
            return search(this.searchSourceBuilder);
        }

    }

}