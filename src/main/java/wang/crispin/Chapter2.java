package wang.crispin;

import com.google.gson.Gson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * 第二张学习代码.
 *
 * @author : chris
 * 2018-07-26
 */
public class Chapter2 {
    private static Jedis conn;

    private Chapter2() {
        conn = new Jedis("101.132.47.22", 6379);
        conn.select(0);
    }

    public static void main(String[] args) throws InterruptedException {
        new Chapter2().run();
    }

    private void run() throws InterruptedException {
//        testLoginCookies();
//        testShopppingCartCookies();
//        testCacheRequest();
        testCacheRows();
    }

    private void testCacheRequest() {
        System.out.println("\n----- testCacheRequest -----");
        String token = UUID.randomUUID().toString();

        CallBack callback = request -> "content for " + request;

        updateToken(token, "username", "itemX");
        String url = "http://test.com/?item=itemX";
        System.out.println("We are going to cache a simple request against " + url);
        String result = cacheRequest(url, callback);
        System.out.println("We got initial content:\n" + result);
        System.out.println();

        assert result != null;

        System.out.println("To test that we've cached the request, we'll pass a bad callback");
        String result2 = cacheRequest(url, null);
        System.out.println("We ended up getting the same response!\n" + result2);

        assert result.equals(result2);

        assert !canCache("http://test.com/");
        assert !canCache("http://test.com/?item=itemX&_=1234536");
    }

    public void testCacheRows() throws InterruptedException {
        System.out.println("\n----- testCacheRows -----");
        System.out.println("First, let's schedule caching of itemX every 5 seconds");
        scheduleRowCache("itemX", 5);
        System.out.println("Our schedule looks like:");
        Set<Tuple> s = conn.zrangeWithScores("schedule:", 0, -1);
        for (Tuple tuple : s) {
            System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert s.size() != 0;

        System.out.println("We'll start a caching thread that will cache the data...");

        CacheRowsThread thread = new CacheRowsThread();
        thread.start();

        Thread.sleep(1000);
        System.out.println("Our cached data looks like:");
        String r = conn.get("inv:itemX");
        System.out.println(r);
        assert r != null;
        System.out.println();

        System.out.println("We'll check again in 5 seconds...");
        Thread.sleep(5000);
        System.out.println("Notice that the data has changed...");
        String r2 = conn.get("inv:itemX");
        System.out.println(r2);
        System.out.println();
        assert r2 != null;
        assert !r.equals(r2);

        System.out.println("Let's force un-caching");
        scheduleRowCache("itemX", -1);
        Thread.sleep(1000);
        r = conn.get("inv:itemX");
        System.out.println("The cache was cleared? " + (r == null));
        assert r == null;

        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The database caching thread is still alive?!?");
        }
    }

    private void testLoginCookies() throws InterruptedException {
        System.out.println("\n----- testLoginCookies -----");
        String token = UUID.randomUUID().toString();

        updateToken(token, "username", "itemX");
        updateToken(UUID.randomUUID().toString(), "chris", "apple");
        System.out.println("We just logged-in/updated token: " + token);
        System.out.println("For user: 'username'");
        System.out.println();

        System.out.println("What username do we get when we look-up that token?");
        String r = checkToken(token);
        System.out.println(r);
        System.out.println();
        assert r != null;

        System.out.println("Let's drop the maximum number of cookies to 0 to clean them out");
        System.out.println("We will start a thread to do the cleaning, while we stop it later");

        CleanSessionsThread thread = new CleanSessionsThread(0);
        thread.setDaemon(true);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The clean sessions thread is still alive?!?");
        }

        long s = conn.hlen("login:");
        System.out.println("The current number of sessions still available is: " + s);
        assert s == 0;
    }

    private void testShopppingCartCookies() throws InterruptedException {
        System.out.println("\n----- testShopppingCartCookies -----");
        String token = UUID.randomUUID().toString();

        System.out.println("We'll refresh our session...");
        updateToken(token, "username", "itemX");
        System.out.println("And add an item to the shopping cart");
        addToCart(token, "itemY", 3);
        Map<String, String> r = conn.hgetAll("cart:" + token);
        System.out.println("Our shopping cart currently has:");
        for (Map.Entry<String, String> entry : r.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        System.out.println();

        assert r.size() >= 1;

        System.out.println("Let's clean out our sessions and carts");
        CleanFullSessionsThread thread = new CleanFullSessionsThread(0);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The clean sessions thread is still alive?!?");
        }

        r = conn.hgetAll("cart:" + token);
        System.out.println("Our shopping cart now contains:");
        for (Map.Entry<String, String> entry : r.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        assert r.size() == 0;
    }

    private String checkToken(String token) {
        return conn.hget("login:", token);
    }

    private void updateToken(String token, String user, String item) {
        long timeStamp = System.currentTimeMillis() / 1000;
        conn.hset("login:", token, user);
        // 记录令牌最后一次出现的时间
        conn.zadd("recent:", timeStamp, token);
        if (item != null) {
            // 记录用户浏览过的商品
            conn.zadd("viewed:" + token, timeStamp, item);
            // 移除旧的记录,只保留用户最近浏览过的25个产品
            conn.zremrangeByRank("viewed:" + token, 0, -26);
            conn.zincrby("viewed:", -1, item);
        }
    }

    private void addToCart(String session, String item, int count) {
        if (count <= 0) {
            conn.hdel("cart:" + session, item);
        } else {
            conn.hset("cart:" + session, item, String.valueOf(count));
        }
    }

    private void scheduleRowCache(String rowId, int delay) {
        conn.zadd("delay:", delay, rowId);
        conn.zadd("schedule:", System.currentTimeMillis() / 1000, rowId);
    }

    private String cacheRequest(String request, CallBack callBack) {
        if (!canCache(request)) {
            return callBack != null ? callBack.call(request) : null;
        }

        String pageKey = "cache:" + hashRequest(request);
        String content = conn.get(pageKey);
        if (content == null && callBack != null) {
            content = callBack.call(request);
            conn.setex(pageKey, 300, content);
        }
        return content;
    }

    private String hashRequest(String request) {
        return String.valueOf(request.hashCode());
    }

    private boolean canCache(String request) {
        try {
            URL url = new URL(request);
            Map<String, String> params = new HashMap<>(16);
            if (url.getQuery() != null) {
                for (String param : url.getQuery().split("&")) {
                    final String[] pair = param.split("=", 2);
                    params.put(pair[0], pair.length == 2 ? pair[1] : null);
                }
            }

            final String itemId = extractItemId(params);
            if (itemId == null || isDynamic(params)) {
                return false;
            }
            final Long rank = conn.zrank("viewed:", itemId);
            return rank != null && rank < 10000;

        } catch (MalformedURLException e) {
            return false;
        }
    }

    private boolean isDynamic(Map<String, String> params) {
        return params.containsKey("_");
    }

    private String extractItemId(Map<String, String> params) {
        return params.get("item");
    }

    public interface CallBack {
        String call(String request);
    }

    public class CleanSessionsThread extends Thread {
        private Jedis conn;
        private int limit;
        private boolean quit;

        public CleanSessionsThread(int limit) {
            this.conn = new Jedis("101.132.47.22", 6379);
            this.conn.select(0);
            this.limit = limit;
        }

        public void quit() {
            quit = true;
        }

        @Override
        public void run() {
            while (!quit) {
                final long size = conn.zcard("recent:");
                if (size <= limit) {
                    try {
                        sleep(1000);
                    } catch (InterruptedException e) {
                        currentThread().interrupt();
                    }
                    continue;
                }

                long endIndex = Math.min(size - limit, 100);
                final Set<String> tokenSet = conn.zrange("recent:", 0, endIndex - 1);
                final String[] tokens = tokenSet.toArray(new String[0]);
                List<String> sessionKeys = new ArrayList<>();
                for (String token : tokens) {
                    sessionKeys.add("viewed:" + token);
                }
                conn.del(sessionKeys.toArray(new String[0]));
                conn.hdel("login:", tokens);
                conn.zrem("recent:", tokens);
            }
        }
    }

    public class CleanFullSessionsThread extends Thread {
        private Jedis conn;
        private int limit;
        private boolean quit;

        private CleanFullSessionsThread(int limit) {
            this.conn = new Jedis("101.132.47.22", 6379);
            this.conn.select(0);
            this.limit = limit;
        }

        public void quit() {
            quit = true;
        }

        @Override
        public void run() {
            while (!quit) {
                final long size = conn.zcard("recent:");
                if (size <= limit) {
                    try {
                        sleep(1000);
                    } catch (InterruptedException e) {
                        currentThread().interrupt();
                    }
                    continue;
                }

                long endIndex = Math.min(size - limit, 100);
                final Set<String> sessionSet = conn.zrange("recent:", 0, endIndex - 1);
                final String[] sessions = sessionSet.toArray(new String[1]);
                List<String> sessionKeys = new ArrayList<>();
                for (String session : sessions) {
                    sessionKeys.add("viewed:" + session);
                    sessionKeys.add("cart:" + session);
                }
                conn.del(sessionKeys.toArray(new String[1]));
                conn.hdel("login:", sessions);
                conn.zrem("recent:", sessions);
            }
        }
    }

    public class CacheRowsThread extends Thread {
        private Jedis conn;
        private boolean quit;

        private CacheRowsThread() {
            this.conn = new Jedis("101.132.47.22", 6379);
            this.conn.select(0);
        }

        public void quit() {
            quit = true;
        }

        @Override
        public void run() {
            Gson gson = new Gson();
            while (!quit) {
                final Set<Tuple> range = conn.zrangeWithScores("schedule:", 0, 0);
                Tuple next = range.size() > 0 ? range.iterator().next() : null;
                long now = System.currentTimeMillis() / 1000;
                if (next == null || next.getScore() > now) {
                    try {
                        sleep(50);
                    } catch (InterruptedException e) {
                        currentThread().interrupt();
                    }
                    continue;
                }

                final String rowId = next.getElement();
                double delay = conn.zscore("delay:", rowId);
                if (delay <= 0) {
                    conn.zrem("delay:", rowId);
                    conn.zrem("schedule:", rowId);
                    conn.del("inv:" + rowId);
                    continue;
                }

                final Inventory row = Inventory.get(rowId);
                conn.zadd("schedule:", now + delay, rowId);
                conn.set("inv:" + rowId, gson.toJson(row));
            }
        }
    }

    public static class Inventory {
        private String id;
        private String data;
        private long time;

        private Inventory(String id) {
            this.id = id;
            this.data = "data to cache...";
            this.time = System.currentTimeMillis() / 1000;
        }

        public static Inventory get(String id) {
            return new Inventory(id);
        }
    }
}
