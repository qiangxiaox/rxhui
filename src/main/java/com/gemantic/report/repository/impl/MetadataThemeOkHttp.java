package com.gemantic.report.repository.impl;

import com.gemantic.springcloud.utils.RestUtils;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.Map;

/**
 * createed By xiaoqiang
 * 2020/1/6 10:25
 */
@Slf4j
@Repository("metadataThemeOkHttp")
public class MetadataThemeOkHttp {

	@Resource
	private OkHttpClient okHttpClient;
	@Value("${metadata.theme.service.url}")
	private String url;


	public String getDictMess(String extraUrl, Long startAt) {
		Map<String, Object> params = Maps.newHashMap();
		params.put("cp", 1);
		params.put("ps", Integer.MAX_VALUE);
		params.put("timeField", "createAt");
		params.put("startAt", startAt);
		StringBuilder urlBuilder = new StringBuilder(url);
		urlBuilder.append(extraUrl);
		if (MapUtils.isNotEmpty(params)) {
			urlBuilder.append("?");
			urlBuilder.append(RestUtils.map2ParamUrlString(params));
		}
		log.info("httpclient get url " + urlBuilder.toString());
		Response response = null;

		String result = null;
		try {
			Request request = (new Request.Builder()).url(urlBuilder.toString()).get().build();
			response = okHttpClient.newCall(request).execute();
			if (response.isSuccessful()) {
				result = response.body().string();
			}
		} catch (Exception e) {
			log.error(urlBuilder.toString(), e);
		} finally {
			if (null != response) {
				response.close();
			}
		}
		return result;
	}

}
