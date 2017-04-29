package org.cisiondata.modules.boot;

import javax.annotation.Resource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootConfiguration
public class SpringBootTest {

	@Resource(name = "redisTemplate")
	private RedisTemplate<String, Object> redisTemplate = null;
	
	@Test
	public void testRedisTemplate() {
		redisTemplate.opsForValue().set("name", "zhangsan");
		System.out.println(redisTemplate.opsForValue().get("name"));
	}
	
}
