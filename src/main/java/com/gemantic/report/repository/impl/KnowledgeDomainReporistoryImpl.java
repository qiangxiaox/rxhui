package com.gemantic.report.repository.impl;

import com.alibaba.fastjson.JSON;
import com.gemantic.dict.client.DictClient;
import com.gemantic.dict.model.Dict;
import com.gemantic.report.repository.KnowledgeDomainReporistory;
import com.gemantic.springcloud.model.PageResponse;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * createed By xiaoqiang
 * 2019/12/12 16:47
 */
@Slf4j
@Repository
public class KnowledgeDomainReporistoryImpl implements KnowledgeDomainReporistory {
	@Value("${kb.doc.service.url}")
	private String kdServiceUrl;
	@Resource
	private OkHttpClient okHttpClient;
	@Resource
	private DictClient dictClient;

	@Override
	public void saveDict(List<Dict> dictList) {
		log.info("保存词典到MongoDB");
		okhttp3.Response response = null;
		StringBuilder urlBuilder = null;
		try {
			urlBuilder = new StringBuilder(kdServiceUrl + "/dict");
			String json = JSON.toJSONString(dictList);
			RequestBody requestBody = FormBody.create(MediaType.parse("application/json; charset=utf-8")
					, json);
			Request request = new Request.Builder()
					.url(urlBuilder.toString()).post(requestBody)
					.build();
			response = okHttpClient.newCall(request).execute();
			log.info("保存词典到MongoDB返回状态码" + response.isSuccessful() + " and " + response.code());
		} catch (Exception e) {
			log.error(urlBuilder.toString(),e);
		} finally {
			if(null != response){
				response.close();
			}
		}
	}

	@Override
	public List<Dict> findPropDict() {
		try {
			Map<String, String> params = Maps.newHashMap();
			params.put("type", "属性");
			params.put("source", "1");
			com.gemantic.springcloud.model.Response<PageResponse<com.gemantic.dict.model.Dict>> dictProp = dictClient.find(null, null, null, null,
					null, null, null, null, null,
					1, Integer.MAX_VALUE, params, null);
			List<Dict> dicts = dictProp.getData().getList();
			return dicts;
		}catch (Exception e){
			log.error("词典属性获取失败--{}", e);
		}
		return null;
	}


}
