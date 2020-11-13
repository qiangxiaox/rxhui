/**
 * 
 */
package com.gemantic.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import javax.annotation.Resource;

/**
 * swagger配置
 * 
 * @author yhye 2016年11月21日下午4:09:11
 */
@Configuration
@EnableSwagger2
@Profile({"local","dev"})
public class Swagger2Config {
	private @Resource Environment environment;

	public @Bean Docket createRestApi() {
		ApiInfo apiInfo = new ApiInfoBuilder().title(environment.getProperty("spring.application.name") + " API")
				.version("1.0").build();
		return new Docket(DocumentationType.SWAGGER_2).apiInfo(apiInfo).select()
				.apis(RequestHandlerSelectors.basePackage("com.gemantic")).paths(PathSelectors.any()).build();
	}
}
