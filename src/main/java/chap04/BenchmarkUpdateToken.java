package chap04;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.lang.reflect.Method;

/**
 * @Author sdh
 * @Date Created in 2019/3/8 10:12
 * @description
 */
public class BenchmarkUpdateToken
{
    public static void main(String[] args)
    {
        BenchmarkUpdateToken benchmarkUpdateToken = new BenchmarkUpdateToken();
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        benchmarkUpdateToken.testBenchmarkUpdateToken(conn);
    }

    public void testBenchmarkUpdateToken(Jedis conn)
    {
        System.out.println("\n----- testBenchmarkUpdate -----");
        benchmarkUpdateToken(conn, 5);
    }

    public void benchmarkUpdateToken(Jedis conn, int duration)
    {
        try
        {
            @SuppressWarnings("rawtypes")
            Class[] args = new Class[]{Jedis.class, String.class, String.class, String.class};

            // 分别执行updateToken和updateTokenPipeline函数
            Method[] methods = new Method[]{
                    this.getClass().getDeclaredMethod("updateToken", args),
                    this.getClass().getDeclaredMethod("updateTokenPipeline", args),
            };

            for (Method method : methods)
            {
                int count = 0;
                long start = System.currentTimeMillis();
                long end = start + (duration * 1000);
                while (System.currentTimeMillis() < end)
                {
                    count++;
                    method.invoke(this, conn, "token", "user", "item");
                }

                // 计算时长并打印结果
                long delta = System.currentTimeMillis() - start;
                System.out.println(method.getName() + ' ' + count + ' ' + (delta / 1000) + ' ' +
                                    (count / (delta / 1000)));
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void updateToken(Jedis conn, String token, String user, String item)
    {
        long timestamp = System.currentTimeMillis() / 1000;
        conn.hset("login:", token, user);
        conn.zadd("recent:", timestamp, token);
        if (item != null)
        {
            conn.zadd("viewed:" + token, timestamp, item);
            conn.zremrangeByRank("viewed:" + token, 0, -26);
            conn.zincrby("viewed:", -1, item);
        }
    }

    /**
     * 流水线形式
     * @param conn
     * @param token
     * @param user
     * @param item
     */
    public void updateTokenPipeline(Jedis conn, String token, String user, String item)
    {
        long timestamp = System.currentTimeMillis() / 1000;

        // 设置流水线
        Pipeline pipe = conn.pipelined();
        pipe.multi();
        pipe.hset("login:", token, user);
        pipe.zadd("recent:", timestamp, token);

        if (item != null)
        {
            pipe.zadd("viewed:" + token, timestamp, item);
            pipe.zremrangeByRank("viewed:" + token, 0, -26);
            pipe.zincrby("viewed:", -1, item);
        }

        // 执行流水线中的命令
        pipe.exec();
    }
}
