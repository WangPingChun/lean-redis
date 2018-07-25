package wang.crispin;

import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * @author : chris
 * 2018-07-25
 */
public class Chapter1 {
    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PER_PAGE = 25;

    public static void main(String[] args) {
        new Chapter1().run();
    }

    public void run() {
        Jedis conn = new Jedis("localhost");
        conn.select(15);
        String articleId = postArticle(conn, "username", "A title", "http://www.google.com");
        System.out.println("We posted a new article with id: " + articleId);
        System.out.println("Its HASH looks like:");
        Map<String, String> articleData = conn.hgetAll("article:" + articleId);
        for (Map.Entry<String, String> entry : articleData.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }

        System.out.println();

//        articleVote(conn, "other_user", "article:" + articleId);
//        String votes = conn.hget("article:" + articleId, "votes");
//        System.out.println("We voted for the article, it now has votes: " + votes);
//        assert Integer.parseInt(votes) > 1;

//        System.out.println("The currently highest-scoring articles are:");
//        List<Map<String,String>> articles = getArticles(conn, 1);
//        printArticles(articles);
//        assert articles.size() >= 1;

//        addGroups(conn, articleId, new String[]{"new-group"});
//        System.out.println("We added the article to a new group, other articles include:");
//        articles = getGroupArticles(conn, "new-group", 1);
//        printArticles(articles);
//        assert articles.size() >= 1;
    }

    void articleVote(Jedis conn, String user, String article) {
        // 计算文章的投票截止时间
        final long cutoff = (System.currentTimeMillis() / 1000) - ONE_WEEK_IN_SECONDS;
        // 检查是否还可以对文章进行投票
        if (conn.zscore("time:", article) < cutoff) {
            return;
        }
        // 取出文章id
        final String articleId = article.substring(article.indexOf(':') + 1);
        if (conn.sadd("voted:" + articleId, user) == 1) {
            conn.zincrby("score:", VOTE_SCORE, article);
            conn.hincrBy(article, "votes", 1);
        }
    }

    String postArticle(Jedis conn, String user, String title, String link) {
        // 生成一个新的文章ID
        final String articleId = conn.incr("article:").toString();
        // 将发布文章的用户添加到文章的已投票用户名单里面
        String voted = "voted:" + articleId;
        conn.sadd(voted, user);
        conn.expire(voted, ONE_WEEK_IN_SECONDS);
        // 将文章信息存储到以一个散列里面
        long now = System.currentTimeMillis() / 1000;
        final String article = "article:" + articleId;
        HashMap<String, String> articleData = new HashMap<>(16);
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");
        conn.hmset(article, articleData);
        // 将文章添加到根据发布时间排序的有序集合和根据评分排序的有序集合里面
        conn.zadd("score:", now + VOTE_SCORE, article);
        conn.zadd("time:", now, article);
        return articleId;
    }

    List<Map<String, String>> getArticles(Jedis conn, int page) {
        return getArticles(conn, page, "score:");
    }

    List<Map<String, String>> getArticles(Jedis conn, int page, String order) {
        int start = (page - 1) * ARTICLES_PER_PAGE;
        int end = start + ARTICLES_PER_PAGE - 1;

        final Set<String> ids = conn.zrevrange(order, start, end);
        List<Map<String, String>> articles = new ArrayList<>();
        for (String id : ids) {
            final Map<String, String> articleData = conn.hgetAll(id);
            articleData.put("id", id);
            articles.add(articleData);
        }

        return articles;
    }


    private void printArticles(List<Map<String, String>> articles) {
        articles.forEach((article) -> {
            System.out.println("  id:  " + article.get("id"));
            article.forEach((key, value) -> {
                if (!key.equals("id")) {
                    System.out.println("    " + key + ": " + value);
                }
            });
        });
    }

}
