package chap01;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

import java.util.*;


/**
 * @Author sdh
 * @Date Created in 2019/3/7 15:13
 * @description
 */
public class ArticleVote
{
    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PER_PAGE = 25;

    public static final void main(String[] args)
    {
        new ArticleVote().run();
    }

    public void run()
    {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        String articleId = postArticle(
                conn, "username", "A title", "http://www.google.com");
        System.out.println("We posted a new article with id: " + articleId);
        System.out.println("Its HASH looks like:");
        Map<String, String> articleData = conn.hgetAll("article:" + articleId);
        for (Map.Entry<String, String> entry : articleData.entrySet())
        {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }

        System.out.println();

        articleVote(conn, "other_user", "article:" + articleId);
        String votes = conn.hget("article:" + articleId, "votes");
        System.out.println("We voted for the article, it now has votes: " + votes);
        assert Integer.parseInt(votes) > 1;

        System.out.println("The currently highest-scoring articles are:");
        List<Map<String, String>> articles = getArticles(conn, 1);
        printArticles(articles);
        assert articles.size() >= 1;

        addGroups(conn, articleId, new String[]{"new-group"});
        System.out.println("We added the article to a new group, other articles include:");
        articles = getGroupArticles(conn, "new-group", 1);
        printArticles(articles);
        assert articles.size() >= 1;
    }

    /**
     * 发表新文章
     *
     * @param conn
     * @param user
     * @param title
     * @param link
     * @return
     */
    public String postArticle(Jedis conn, String user, String title, String link)
    {
        // 生成一个新文章Id
        String articleId = String.valueOf(conn.incr("article:"));

        String voted = "voted:" + articleId;

        // 把发布该文章的用户添加至已投票用户名单
        conn.sadd(voted, user);

        // 设置过期时间
        conn.expire(voted, ONE_WEEK_IN_SECONDS);

        // 存储文章全部信息
        long now = System.currentTimeMillis() / 1000;
        String article = "article:" + articleId;
        HashMap<String, String> articleData = new HashMap<String, String>();
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");
        conn.hmset(article, articleData);

        // 将文章添加到按评分排序和按时间排序的有序集合中
        conn.zadd("score:", now + VOTE_SCORE, article);
        conn.zadd("time:", now, article);

        return articleId;
    }

    /**
     * 给文章投票
     *
     * @param conn
     * @param user
     * @param article
     */
    public void articleVote(Jedis conn, String user, String article)
    {
        // 计算文章投票时间是否截止
        long cutoff = (System.currentTimeMillis() / 1000) - ONE_WEEK_IN_SECONDS;
        if (conn.zscore("time:", article) < cutoff)
        {
            return;
        }

        // 获取文章Id
        String articleId = article.substring(article.indexOf(':') + 1);

        // 如果当前用户是第一次为该文章投票，则加分
        if (conn.sadd("voted:" + articleId, user) == 1)
        {
            conn.zincrby("score:", VOTE_SCORE, article);
            conn.hincrBy(article, "votes", 1);
        }
    }


    public List<Map<String, String>> getArticles(Jedis conn, int page)
    {
        return getArticles(conn, page, "score:");
    }

    /**
     * 获取文章列表
     *
     * @param conn
     * @param page
     * @param order
     * @return
     */
    public List<Map<String, String>> getArticles(Jedis conn, int page, String order)
    {
        // 获取文章的起始和结束位置索引
        int start = (page - 1) * ARTICLES_PER_PAGE;
        int end = start + ARTICLES_PER_PAGE - 1;

        // 获取多个文章Id
        Set<String> ids = conn.zrevrange(order, start, end);

        List<Map<String, String>> articles = new ArrayList<Map<String, String>>();
        for (String id : ids)
        {
            // 根据Id获取文章全部信息并加入list
            Map<String, String> articleData = conn.hgetAll(id);
            articleData.put("id", id);
            articles.add(articleData);
        }

        return articles;
    }

    /**
     * 把文章加入群组
     *
     * @param conn
     * @param articleId
     * @param toAdd
     */
    public void addGroups(Jedis conn, String articleId, String[] toAdd)
    {
        // 构建文章键名
        String article = "article:" + articleId;

        // 把文章加入群组
        for (String group : toAdd)
        {
            conn.sadd("group:" + group, article);
        }
    }

    public List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page)
    {
        return getGroupArticles(conn, group, page, "score:");
    }

    /**
     * 获取一整页文章
     *
     * @param conn
     * @param group
     * @param page
     * @param order
     * @return
     */
    public List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page, String order)
    {
        // 为每个群组的每种排列顺序都创建一个键
        String key = order + group;

        // 如果没有缓存结果则重新排序
        if (!conn.exists(key))
        {
            ZParams params = new ZParams().aggregate(ZParams.Aggregate.MAX);
            conn.zinterstore(key, params, "group:" + group, order);

            // 结果缓存60秒，之后自动删除
            conn.expire(key, 60);
        }
        return getArticles(conn, page, key);
    }

    private void printArticles(List<Map<String, String>> articles)
    {
        for (Map<String, String> article : articles)
        {
            System.out.println("  id: " + article.get("id"));
            for (Map.Entry<String, String> entry : article.entrySet())
            {
                if (entry.getKey().equals("id"))
                {
                    continue;
                }
                System.out.println("    " + entry.getKey() + ": " + entry.getValue());
            }
        }
    }
}
