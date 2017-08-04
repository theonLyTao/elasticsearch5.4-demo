package com.theonlytao.search.template;

import lombok.Data;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;

/**
 * Created by T T on 2017/6/27.
 */
@Data
public class SearchTemplate{
    private Client client;

    /**
     * 批量创建文档，需指定索引和类型
     * @param index 索引
     * @param type  类型
     * @param docs  文档
     * @return
     */
    public BulkResponse createIndex(String index, String type, List<String> docs){
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for(String doc : docs){
            bulkRequestBuilder.add(client.prepareIndex(index,type).setSource(doc));
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        return bulkResponse;
    }

    /**
     * 单个创建文档，需指定索引和类型
     * @param index 索引
     * @param type  类型
     * @param doc   文档
     * @return
     */
    public IndexResponse createIndex(String index, String type, String doc){
        IndexResponse indexResponse = client.prepareIndex(index, type).setSource(doc).get();
        return indexResponse;
    }

    /**
     * 指定索引、类型和搜索类型进行搜索，若类型为空默认对整个索引进行搜索
     * @param index 索引
     * @param type  类型
     * @param queryBuilder  搜索类型
     * @return
     */
    public SearchResponse search(String index, String type, QueryBuilder queryBuilder){
        if (type == null|| type.length() == 0){
            return client.prepareSearch(index).setQuery(queryBuilder).get();
        }
        return client.prepareSearch(index).setTypes(type).setQuery(queryBuilder).get();
    }

    /**
     * 根据id删除指定索引、类型下的文档，id需先通过搜索获取
     * @param index 索引
     * @param type  类型
     * @param id    文档id
     * @return
     */
    public DeleteResponse deleteIndex(String index,String type,String id) {
        DeleteResponse deleteResponse= client.prepareDelete(index, type, id).get();
        return deleteResponse;
    }
    /**
     * 删除指定索引，慎用
     * @param index 索引
     * @return
     */
    public DeleteIndexResponse deleteAll(String index){
        DeleteIndexResponse deleteIndexResponse = client.admin().indices().prepareDelete(index).get();
        return deleteIndexResponse;
    }

    /**
     * 根据index、type、id更新文档
     * @param index 索引
     * @param type  文档
     * @param id    文档id
     * @param newDoc 更新后的文档json字符串
     * @return
     */
    public Boolean update(String index,String type,String id,String newDoc){
        UpdateResponse updateResponse = client.prepareUpdate(index,type,id).setDoc(newDoc).get();
        if(updateResponse.getResult()!= DocWriteResponse.Result.UPDATED){
            return false;
        }
        return true;
    }

    /**
     * 判断指定Index是否存在
     * @param index
     * @return
     */
    public Boolean indexExist(String index){
        IndicesExistsRequest request = new IndicesExistsRequest(index);
        IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
        if (response.isExists()) {
            return true;
        }
        return false;
    }
}
