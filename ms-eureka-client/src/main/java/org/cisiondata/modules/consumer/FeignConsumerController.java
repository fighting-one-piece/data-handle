package org.cisiondata.modules.consumer;

import org.cisiondata.modules.consumer.feign.FeignConsumerClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

//@RestController
public class FeignConsumerController {

	@Autowired
	private FeignConsumerClient feignConsumerClient = null;
	
	@RequestMapping(value = "/feign/consume/{operation}", method = RequestMethod.GET)
	public int doConsume(@PathVariable String operation, int a, int b) {
		System.out.println("a: " + a + " b: " + b);
		System.out.println("feignConsumerClient: " + feignConsumerClient);
		return feignConsumerClient.add(a, b);
	}
	
}
