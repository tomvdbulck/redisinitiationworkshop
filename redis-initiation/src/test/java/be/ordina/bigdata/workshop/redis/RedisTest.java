package be.ordina.bigdata.workshop.redis;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;

public class RedisTest {

	Jedis jedis;

	private static final String REDIS_TEST_KEY = "myTestKey";
	
	@Before
	public void init(){
		Redis redis = new Redis();
		jedis = redis.getRedisClient();
	}
	
	@After
	public void destroy(){
		jedis.del(REDIS_TEST_KEY);
		jedis.close();
	}
	
	@Test
	public void testConnection(){
		//Successful if no exception is thrown during init.
	}
	
	
	@Test
	public void testString() {
		assertFalse(jedis.exists(REDIS_TEST_KEY));
		
		String value = "blablabla test blablahaha";
		// set the data in redis string
		jedis.set(REDIS_TEST_KEY, value);

		assertTrue(jedis.exists(REDIS_TEST_KEY));
		assertEquals(value, jedis.get(REDIS_TEST_KEY));
	}

	@Test
	public void testList(){
		assertFalse(jedis.exists(REDIS_TEST_KEY));
		
		//store data in redis list
		for (int i = 0; i < 50; i++) {
			jedis.rpush(REDIS_TEST_KEY, "Value " + i);
		}
		
		assertTrue(jedis.exists(REDIS_TEST_KEY));
		
	     // Get the stored data and print it
	     List<String> list = jedis.lrange(REDIS_TEST_KEY, 11 ,15);
	      
	     assertEquals(5, list.size());
	     assertEquals("Value 11", list.get(0));
	     assertEquals("Value 15", list.get(4));
	}
}
