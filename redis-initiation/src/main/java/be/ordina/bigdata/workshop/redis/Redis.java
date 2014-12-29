package be.ordina.bigdata.workshop.redis;

import java.util.logging.Level;
import java.util.logging.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * Hello world!
 *
 */
public class Redis {

	private static final String REDIS_HOST_URL = "localhost";
	private static final int REDIS_PORT = 6379;

	private static final Logger log = Logger.getLogger(Redis.class.getName());

	private Jedis jedisInstance = null;

	public Jedis getRedisClient() {
		if (jedisInstance == null) {
			jedisInstance = startConnection();
		}

		return jedisInstance;
	}

	private Jedis startConnection() {
		try {
			// Connecting to Redis server on localhost
			log.info("Start connection to redis.");
			Jedis jedis = new Jedis(REDIS_HOST_URL, REDIS_PORT);

			// check whether server is running or not
			log.info("Test server status with ping: " + jedis.ping());

			log.info("Succesfully connected to redis on : " + REDIS_HOST_URL + ":" + REDIS_PORT);

			return jedis;
		} catch (JedisConnectionException e) {
			log.log(Level.SEVERE, "Can't connect to Redis on : " + REDIS_HOST_URL + ":" + REDIS_PORT, e);
			throw e;
		}
	}
}
