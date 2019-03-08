package chap02;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @Author sdh
 * @Date Created in 2019/3/7 16:17
 * @description
 */
public class ShopppingCart
{
    public static void main(String[] args) throws InterruptedException
    {
        ShopppingCart shopppingCart = new ShopppingCart();
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        shopppingCart.testShopppingCartCookies(conn);
    }

    public void testShopppingCartCookies(Jedis conn) throws InterruptedException
    {
        System.out.println("\n----- testShopppingCartCookies -----");
        String token = UUID.randomUUID().toString();

        System.out.println("We'll refresh our session...");
        updateToken(conn, token, "username", "itemX");

        System.out.println("And add an item to the shopping cart");
        addToCart(conn, token, "itemY", 3);

        Map<String, String> r = conn.hgetAll("cart:" + token);
        System.out.println("Our shopping cart currently has:");
        for (Map.Entry<String, String> entry : r.entrySet())
        {
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
        if (thread.isAlive())
        {
            throw new RuntimeException("The clean sessions thread is still alive?!?");
        }

        r = conn.hgetAll("cart:" + token);
        System.out.println("Our shopping cart now contains:");
        for (Map.Entry<String, String> entry : r.entrySet())
        {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        assert r.size() == 0;
    }

    /**
     * 更新购物车
     * @param conn
     * @param session
     * @param item
     * @param count
     */
    public void addToCart(Jedis conn, String session, String item, int count)
    {
        // 数量小于等于0则删除该商品
        if (count <= 0)
        {
            conn.hdel("cart:" + session, item);
        }

        // 否则更新数量
        else
        {
            conn.hset("cart:" + session, item, String.valueOf(count));
        }
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
            conn.zincrby("viewed:", -1, item);
        }
    }

    public class CleanFullSessionsThread extends Thread
    {
        private Jedis conn;
        private int limit;
        private boolean quit;

        public CleanFullSessionsThread(int limit)
        {
            this.conn = new Jedis("localhost");
            this.conn.select(15);
            this.limit = limit;
        }

        public void quit()
        {
            quit = true;
        }

        public void run()
        {
            while (!quit)
            {
                long size = conn.zcard("recent:");
                if (size <= limit)
                {
                    try
                    {
                        sleep(1000);
                    }
                    catch (InterruptedException ie)
                    {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                long endIndex = Math.min(size - limit, 100);
                Set<String> sessionSet = conn.zrange("recent:", 0, endIndex - 1);
                String[] sessions = sessionSet.toArray(new String[sessionSet.size()]);

                ArrayList<String> sessionKeys = new ArrayList<String>();
                for (String sess : sessions)
                {
                    sessionKeys.add("viewed:" + sess);
                    sessionKeys.add("cart:" + sess);
                }

                conn.del(sessionKeys.toArray(new String[sessionKeys.size()]));
                conn.hdel("login:", sessions);
                conn.zrem("recent:", sessions);
            }
        }
    }
}
