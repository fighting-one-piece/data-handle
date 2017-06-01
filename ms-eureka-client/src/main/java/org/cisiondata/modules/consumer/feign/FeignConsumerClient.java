package org.cisiondata.modules.consumer.feign;

import org.springframework.cloud.netflix.feign.FeignClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(value = "EUREKA-CLIENT:10001", fallback = FeignConsumerClientHystrix.class)
public interface FeignConsumerClient {

	@RequestMapping(value = "add", method = RequestMethod.GET)
	public int add(@RequestParam(value = "a") int a, @RequestParam(value = "b") int b);
	
}
