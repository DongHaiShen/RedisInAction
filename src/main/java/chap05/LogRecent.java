package chap05;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Date;
import java.util.List;

/**
 * @Author sdh
 * @Date Created in 2019/3/8 10:33
 * @description
 */
public class LogRecent
{
    public static void main(String[] args)
    {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        LogRecent logRecent = new LogRecent();
        logRecent.testLogRecent(conn);
    }

    public void testLogRecent(Jedis conn)
    {
        System.out.println("\n----- testLogRecent -----");
        System.out.println("Let's write a few logs to the recent log");
        for (int i = 0; i < 5; i++)
        {
            logRecent(conn, "test", "this is message " + i);
        }

        List<String> recent = conn.lrange("recent:test:info", 0, -1);
        System.out.println("The current recent message log has this many messages: " + recent.size());
        System.out.println("Those messages include:");
        for (String message : recent)
        {
            System.out.println(message);
        }
        assert recent.size() >= 5;
    }

    public void logRecent(Jedis conn, String name, String message)
    {
        logRecent(conn, name, message, Constant.INFO);
    }

    /**
     * 记录新日志
     *
     * @param conn
     * @param name
     * @param message
     * @param severity
     */
    public void logRecent(Jedis conn, String name, String message, String severity)
    {
        String destination = "recent:" + name + ':' + severity;

        // 流水线操作
        Pipeline pipe = conn.pipelined();

        // 加入时间信息，并加入到列表最前面(lpush)
        pipe.lpush(destination, Constant.TIMESTAMP.format(new Date()) + ' ' + message);

        // 只包含前100个最新消息
        pipe.ltrim(destination, 0, 99);
        pipe.sync();
    }
}
