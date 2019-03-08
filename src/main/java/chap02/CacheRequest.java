package chap02;

import redis.clients.jedis.Jedis;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @Author sdh
 * @Date Created in 2019/3/7 16:38
 * @description
 */
public class CacheRequest
{
    public static void main(String[] args)
    {
        CacheRequest cacheRequest = new CacheRequest();
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        cacheRequest.testCacheRequest(conn);
    }

    public void testCacheRequest(Jedis conn)
    {
        System.out.println("\n----- testCacheRequest -----");
        String token = UUID.randomUUID().toString();

        Callback callback = new Callback()
        {
            public String call(String request)
            {
                return "content for " + request;
            }
        };

        updateToken(conn, token, "username", "itemX");
        String url = "http://test.com/?item=itemX";
        System.out.println("We are going to cache a simple request against " + url);

        String result = cacheRequest(conn, url, callback);
        System.out.println("We got initial content:\n" + result);
        System.out.println();

        assert result != null;

        System.out.println("To test that we've cached the request, we'll pass a bad callback");
        String result2 = cacheRequest(conn, url, null);
        System.out.println("We ended up getting the same response!\n" + result2);

        assert result.equals(result2);

        assert !canCache(conn, "http://test.com/");
        assert !canCache(conn, "http://test.com/?item=itemX&_=1234536");
    }

    /**
     * 更新令牌
     *
     * @param conn
     * @param token
     * @param user
     * @param item
     */
    public void updateToken(Jedis conn, String token, String user, String item)
    {
        long timestamp = System.currentTimeMillis() / 1000;

        // 令牌与已登录用户的映射
        conn.hset("login:", token, user);

        // 记录令牌最后一次出现时间
        conn.zadd("recent:", timestamp, token);

        if (item != null)
        {
            // 记录浏览的商品
            conn.zadd("viewed:" + token, timestamp, item);

            // 移除旧记录，只保留25个
            conn.zremrangeByRank("viewed:" + token, 0, -26);

            // 记录浏览次数并排序
            conn.zincrby("viewed:", -1, item);
        }
    }

    public interface Callback
    {
        public String call(String request);
    }

    public String extractItemId(Map<String, String> params)
    {
        return params.get("item");
    }

    public boolean isDynamic(Map<String, String> params)
    {
        return params.containsKey("_");
    }

    public String hashRequest(String request)
    {
        return String.valueOf(request.hashCode());
    }

    public boolean canCache(Jedis conn, String request)
    {
        try
        {
            URL url = new URL(request);
            HashMap<String, String> params = new HashMap<String, String>();
            if (url.getQuery() != null)
            {
                for (String param : url.getQuery().split("&"))
                {
                    String[] pair = param.split("=", 2);
                    params.put(pair[0], pair.length == 2 ? pair[1] : null);
                }
            }

            String itemId = extractItemId(params);
            if (itemId == null || isDynamic(params))
            {
                return false;
            }
            Long rank = conn.zrank("viewed:", itemId);
            return rank != null && rank < 10000;
        }
        catch (MalformedURLException mue)
        {
            return false;
        }
    }

    /**
     * 处理缓存请求
     *
     * @param conn
     * @param request
     * @param callback
     * @return
     */
    public String cacheRequest(Jedis conn, String request, Callback callback)
    {
        // 不能被缓存，调用回调函数
        if (!canCache(conn, request))
        {
            return callback != null ? callback.call(request) : null;
        }

        // 把请求转换为字符串键
        String pageKey = "cache:" + hashRequest(request);

        // 尝试进行查找
        String content = conn.get(pageKey);

        // 页面没有被缓存，那么生成页面
        if (content == null && callback != null)
        {
            // 将新生成的页面进行缓存5分钟
            content = callback.call(request);
            conn.setex(pageKey, 300, content);
        }

        return content;
    }
}
