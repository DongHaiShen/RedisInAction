package chap05;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

import java.util.*;

/**
 * @Author sdh
 * @Date Created in 2019/3/8 13:24
 * @description
 */
public class States
{
    public static void main(String[] args) throws InterruptedException
    {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        States states = new States();
        states.testStats(conn);
        states.testAccessTime(conn);
    }

    public void testStats(Jedis conn)
    {
        System.out.println("\n----- testStats -----");
        System.out.println("Let's add some data for our statistics!");
        List<Object> r = null;
        for (int i = 0; i < 5; i++)
        {
            double value = (Math.random() * 11) + 5;
            r = updateStats(conn, "temp", "example", value);
        }

        System.out.println("We have some aggregate statistics: " + r);
        Map<String, Double> stats = getStats(conn, "temp", "example");
        System.out.println("Which we can also fetch manually:");
        System.out.println(stats);
        assert stats.get("count") >= 5;
    }

    public void testAccessTime(Jedis conn) throws InterruptedException
    {
        System.out.println("\n----- testAccessTime -----");
        System.out.println("Let's calculate some access times...");
        AccessTimer timer = new AccessTimer(conn);
        for (int i = 0; i < 10; i++)
        {
            timer.start();
            Thread.sleep((int) ((.5 + Math.random()) * 1000));
            timer.stop("req-" + i);
        }

        System.out.println("The slowest access times are:");
        Set<Tuple> atimes = conn.zrevrangeWithScores("slowest:AccessTime", 0, -1);
        for (Tuple tuple : atimes)
        {
            System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert atimes.size() >= 10;
        System.out.println();
    }

    public List<Object> updateStats(Jedis conn, String context, String type, double value)
    {
        int timeout = 5000;
        String destination = "stats:" + context + ':' + type;
        String startKey = destination + ":start";
        long end = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < end)
        {
            conn.watch(startKey);
            String hourStart = Constant.ISO_FORMAT.format(new Date());

            String existing = conn.get(startKey);
            Transaction trans = conn.multi();
            if (existing != null && Constant.COLLATOR.compare(existing, hourStart) < 0)
            {
                trans.rename(destination, destination + ":last");
                trans.rename(startKey, destination + ":pstart");
                trans.set(startKey, hourStart);
            }

            String tkey1 = UUID.randomUUID().toString();
            String tkey2 = UUID.randomUUID().toString();
            trans.zadd(tkey1, value, "min");
            trans.zadd(tkey2, value, "max");

            trans.zunionstore(destination, new ZParams().aggregate(ZParams.Aggregate.MIN), destination, tkey1);
            trans.zunionstore(destination, new ZParams().aggregate(ZParams.Aggregate.MAX), destination, tkey2);

            trans.del(tkey1, tkey2);
            trans.zincrby(destination, 1, "count");
            trans.zincrby(destination, value, "sum");
            trans.zincrby(destination, value * value, "sumsq");

            List<Object> results = trans.exec();
            if (results == null)
            {
                continue;
            }
            return results.subList(results.size() - 3, results.size());
        }
        return null;
    }

    public Map<String, Double> getStats(Jedis conn, String context, String type)
    {
        String key = "stats:" + context + ':' + type;
        Map<String, Double> stats = new HashMap<String, Double>();
        Set<Tuple> data = conn.zrangeWithScores(key, 0, -1);
        for (Tuple tuple : data)
        {
            stats.put(tuple.getElement(), tuple.getScore());
        }
        stats.put("average", stats.get("sum") / stats.get("count"));
        double numerator = stats.get("sumsq") - Math.pow(stats.get("sum"), 2) / stats.get("count");
        double count = stats.get("count");
        stats.put("stddev", Math.pow(numerator / (count > 1 ? count - 1 : 1), .5));
        return stats;
    }

    /**
     * 上下文管理器
     */
    public class AccessTimer
    {
        private Jedis conn;
        private long start;

        public AccessTimer(Jedis conn)
        {
            this.conn = conn;
        }

        public void start()
        {
            start = System.currentTimeMillis();
        }

        public void stop(String context)
        {
            long delta = System.currentTimeMillis() - start;
            List<Object> stats = updateStats(conn, context, "AccessTime", delta / 1000.0);
            double average = (Double) stats.get(1) / (Double) stats.get(0);

            Transaction trans = conn.multi();
            trans.zadd("slowest:AccessTime", average, context);
            trans.zremrangeByRank("slowest:AccessTime", 0, -101);
            trans.exec();
        }
    }
}
