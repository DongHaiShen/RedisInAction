package chap02;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;

/**
 * @Author sdh
 * @Date Created in 2019/3/7 16:03
 * @description
 */
public class LoginCookies
{
    public static void main(String[] args) throws InterruptedException
    {
        LoginCookies loginCookies = new LoginCookies();
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        loginCookies.testLoginCookies(conn);
    }

    public void testLoginCookies(Jedis conn) throws InterruptedException
    {
        System.out.println("\n----- testLoginCookies -----");
        String token = UUID.randomUUID().toString();

        updateToken(conn, token, "username", "itemX");
        System.out.println("We just logged-in/updated token: " + token);
        System.out.println("For user: 'username'");
        System.out.println();

        System.out.println("What username do we get when we look-up that token?");
        String r = checkToken(conn, token);
        System.out.println(r);
        System.out.println();
        assert r != null;

        System.out.println("Let's drop the maximum number of cookies to 0 to clean them out");
        System.out.println("We will start a thread to do the cleaning, while we stop it later");

        CleanSessionsThread thread = new CleanSessionsThread(0);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive())
        {
            throw new RuntimeException("The clean sessions thread is still alive?!?");
        }

        long s = conn.hlen("login:");
        System.out.println("The current number of sessions still available is: " + s);
        assert s == 0;
    }

    /**
     * 尝试获取并返回令牌对应用户
     *
     * @param conn
     * @param token
     * @return
     */
    public String checkToken(Jedis conn, String token)
    {
        return conn.hget("login:", token);
    }

    /**
     * 更新令牌
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

    public class CleanSessionsThread extends Thread
    {
        private Jedis conn;
        private int limit;
        private boolean quit;

        public CleanSessionsThread(int limit)
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
                // 获取已有令牌数量
                long size = conn.zcard("recent:");

                // 数量未超过限制，休眠1秒然后重新检查
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

                // 获取需要删除的令牌Id
                long endIndex = Math.min(size - limit, 100);
                Set<String> tokenSet = conn.zrange("recent:", 0, endIndex - 1);
                String[] tokens = tokenSet.toArray(new String[tokenSet.size()]);

                // 构建键名
                ArrayList<String> sessionKeys = new ArrayList<String>();
                for (String token : tokens)
                {
                    sessionKeys.add("viewed:" + token);
                }

                // 删除最旧的那些令牌
                conn.del(sessionKeys.toArray(new String[sessionKeys.size()]));
                conn.hdel("login:", tokens);
                conn.zrem("recent:", tokens);
            }
        }
    }
}
