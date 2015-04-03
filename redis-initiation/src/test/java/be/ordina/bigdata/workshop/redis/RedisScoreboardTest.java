package be.ordina.bigdata.workshop.redis;

import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;

public class RedisScoreboardTest {
	
	private static final String COLLECTION_NAME = "scores";
	private static final int NR_OF_SCORES_TO_ADD = 1000000;
	Jedis jedis;

	@Before
	public void init(){
		Redis redis = new Redis();
		jedis = redis.getRedisClient();
	}
	
	@After
	public void destroy(){
		jedis.close();
	}
	
	@Test
	public void testConnection(){
		//Successful if no exception is thrown during init.
	}
	
	
	@Test
	public void addScores(){
		Pipeline p = jedis.pipelined();
		Random r = new Random();
		
		for (int i = 0; i < NR_OF_SCORES_TO_ADD; i++) {
			String user = "user" + ":" + r.nextInt(1000);
			double score = r.nextDouble() * 100;

			p.zincrby(COLLECTION_NAME, score, user);
		}
		p.sync();
		System.out.println("added " + NR_OF_SCORES_TO_ADD + " scores.");
	}
	
	@Test
	public void get10HighestScores(){
		Set<Tuple> elements = jedis.zrevrangeWithScores(COLLECTION_NAME, 0, 9);
		
		Assert.assertEquals(10, elements.size());
		
		System.out.println("----  HIGHSCORES  ----");
		int i = 0;
		for (Tuple tuple : elements) {
			System.out.println(++i + ")     " + tuple.getElement() + " has score " + tuple.getScore());
		}
	}
	
	@Test
	public void get10LowestScores(){
		Set<Tuple> elements = jedis.zrangeWithScores(COLLECTION_NAME, 0, 9);
		
		Assert.assertEquals(10, elements.size());
		
		System.out.println("----  LOWSCORES  ----");
		int i = 0;
		for (Tuple tuple : elements) {
			System.out.println(++i + ")     " + tuple.getElement() + " has score " + tuple.getScore());
		}
	}
	
	@Test
	public void clearSet(){
		Long nrOfElements = jedis.zcard(COLLECTION_NAME);
		
		System.out.println(COLLECTION_NAME + " contains " + nrOfElements + " elements.");
//		Assert.assertTrue(nrOfElements > 0);
		
		jedis.del(COLLECTION_NAME);
		
		nrOfElements = jedis.zcard(COLLECTION_NAME);
		
		System.out.println(COLLECTION_NAME + " contains " + nrOfElements + " elements.");
		Assert.assertTrue(nrOfElements == 0);
		
	}
	
}

