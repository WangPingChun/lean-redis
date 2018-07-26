package wang.crispin;

import redis.clients.jedis.Jedis;

/**
 *
 * 第二章练习.
 * @author : WangPingChun
 * 2018-07-26
 */
public class Chapter2 {
	public static void main(String[] args) {
		new Chapter2().run();
	}

	private void run() {

	}

	private String checkToken(Jedis conn, String token) {
		// 尝试获取并返回令牌对应的用户
		return conn.hget("login:", token);
	}
}
