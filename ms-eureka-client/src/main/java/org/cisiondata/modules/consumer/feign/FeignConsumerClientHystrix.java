package org.cisiondata.modules.consumer.feign;

import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestParam;

@Component
public class FeignConsumerClientHystrix implements FeignConsumerClient {

	@Override
	public int add(@RequestParam(value = "a") int a, @RequestParam(value = "b") int b) {
		return -1;
	}

	
}
