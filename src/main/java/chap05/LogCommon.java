package chap05;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * @Author sdh
 * @Date Created in 2019/3/8 10:47
 * @description
 */
public class LogCommon
{
    public static void main(String[] args)
    {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        LogCommon logCommon = new LogCommon();
        logCommon.testLogCommon(conn);
    }

    public void testLogCommon(Jedis conn)
    {
        System.out.println("\n----- testLogCommon -----");
        System.out.println("Let's write some items to the common log");
        for (int count = 1; count < 6; count++)
        {
            for (int i = 0; i < count; i++)
            {
                logCommon(conn, "test", "message-" + count);
            }
        }
        Set<Tuple> common = conn.zrevrangeWithScores("common:test:info", 0, -1);
        System.out.println("The current number of common messages is: " + common.size());
        System.out.println("Those common messages are:");
        for (Tuple tuple : common)
        {
            System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert common.size() >= 5;
    }

    public void logCommon(Jedis conn, String name, String message)
    {
        logCommon(conn, name, message, Constant.INFO, 5000);
    }

    /**
     * 记录并轮换最常见的日志信息
     *
     * @param conn
     * @param name
     * @param message
     * @param severity
     * @param timeout
     */
    public void logCommon(Jedis conn, String name, String message, String severity, int timeout)
    {
        String commonDest = "common:" + name + ':' + severity;
        String startKey = commonDest + ":start";

        // 每timeout时间计算一次
        long end = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < end)
        {
            // 监视时间键
            conn.watch(startKey);
            String hourStart = Constant.ISO_FORMAT.format(new Date());
            String existing = conn.get(startKey);

            // 创建事务
            Transaction trans = conn.multi();
            if (existing != null && Constant.COLLATOR.compare(existing, hourStart) < 0)
            {
                // 归档旧信息
                trans.rename(commonDest, commonDest + ":last");
                trans.rename(startKey, commonDest + ":pstart");

                // 更新时间
                trans.set(startKey, hourStart);
            }

            // 计数器自增
            trans.zincrby(commonDest, 1, message);

            String recentDest = "recent:" + name + ':' + severity;
            trans.lpush(recentDest, Constant.TIMESTAMP.format(new Date()) + ' ' + message);
            trans.ltrim(recentDest, 0, 99);
            List<Object> results = trans.exec();
            if (results == null)
            {
                continue;
            }
            return;
        }
    }
}
