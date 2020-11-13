package com.gemantic.report.repository;

import com.gemantic.semantic.model.CheckPoint;
import com.gemantic.springcloud.model.Response;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * Created by zhj12388 on 2018/6/4.
 */
@FeignClient(name = "checkPointRepository", url = "${semantic.db.service.url}")
public interface CheckPointRepository {
    @RequestMapping(method = RequestMethod.GET, path = "/check/point")
    ResponseEntity<Response<CheckPoint>> getCheckPoint(@RequestParam(name = "name") String name) throws Exception;
    @RequestMapping(method = RequestMethod.POST, path = "/check/point")
    Response<Boolean> saveCheckPoint(@RequestBody CheckPoint checkPoint) throws Exception;
}
