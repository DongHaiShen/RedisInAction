package chap05;

import javafx.util.Pair;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.*;

/**
 * @Author sdh
 * @Date Created in 2019/3/8 10:58
 * @description
 */
public class TimeCounters
{
    public static void main(String[] args) throws InterruptedException
    {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        TimeCounters timeCounters = new TimeCounters();
        timeCounters.testCounters(conn);
    }

    public void testCounters(Jedis conn)
            throws InterruptedException
    {
        System.out.println("\n----- testCounters -----");
        System.out.println("Let's update some counters for now and a little in the future");
        long now = System.currentTimeMillis() / 1000;
        for (int i = 0; i < 10; i++)
        {
            int count = (int) (Math.random() * 5) + 1;
            updateCounter(conn, "test", count, now + i);
        }

        List<Pair<Integer, Integer>> counter = getCounter(conn, "test", 1);
        System.out.println("We have some per-second counters: " + counter.size());
        System.out.println("These counters include:");
        for (Pair<Integer, Integer> count : counter)
        {
            System.out.println("  " + count);
        }
        assert counter.size() >= 10;

        counter = getCounter(conn, "test", 5);
        System.out.println("We have some per-5-second counters: " + counter.size());
        System.out.println("These counters include:");
        for (Pair<Integer, Integer> count : counter)
        {
            System.out.println("  " + count);
        }
        assert counter.size() >= 2;
        System.out.println();

        System.out.println("Let's clean out some counters by setting our sample count to 0");
        CleanCountersThread thread = new CleanCountersThread(0, 2 * 86400000);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        thread.interrupt();
        counter = getCounter(conn, "test", 86400);
        System.out.println("Did we clean out all of the counters? " + (counter.size() == 0));
        assert counter.size() == 0;
    }


    public void updateCounter(Jedis conn, String name, int count)
    {
        updateCounter(conn, name, count, System.currentTimeMillis() / 1000);
    }


    // 更新计数器
    public void updateCounter(Jedis conn, String name, int count, long now)
    {
        Transaction trans = conn.multi();

        // 为每种精度都创建一个计数器
        for (int prec : Constant.PRECISION)
        {
            // 当前时间片开始时间
            long pnow = (now / prec) * prec;
            String hash = String.valueOf(prec) + ':' + name;
            trans.zadd("known:", 0, hash);
            trans.hincrBy("count:" + hash, String.valueOf(pnow), count);
        }
        trans.exec();
    }

    /**
     * 获取计数结果
     *
     * @param conn
     * @param name
     * @param precision
     * @return
     */
    public List<Pair<Integer, Integer>> getCounter(Jedis conn, String name, int precision)
    {
        // 获取键名
        String hash = String.valueOf(precision) + ':' + name;

        // 取出计数器
        Map<String, String> data = conn.hgetAll("count:" + hash);
        ArrayList<Pair<Integer, Integer>> results = new ArrayList<Pair<Integer, Integer>>();
        for (Map.Entry<String, String> entry : data.entrySet())
        {
            results.add(new Pair<Integer, Integer>(
                    Integer.parseInt(entry.getKey()),
                    Integer.parseInt(entry.getValue())));
        }

        //
        Collections.sort(results, (o1, o2) -> (o1.getKey().compareTo(o2.getKey())));
        return results;
    }

    /**
     * 清理程序
     */
    public class CleanCountersThread extends Thread
    {
        private Jedis conn;
        private int sampleCount = 100;
        private boolean quit;
        private long timeOffset;

        public CleanCountersThread(int sampleCount, long timeOffset)
        {
            this.conn = new Jedis("localhost");
            this.conn.select(15);
            this.sampleCount = sampleCount;
            this.timeOffset = timeOffset;
        }

        public void quit()
        {
            quit = true;
        }

        public void run()
        {
            int passes = 0;
            while (!quit)
            {
                long start = System.currentTimeMillis() + timeOffset;
                int index = 0;
                while (index < conn.zcard("known:"))
                {
                    Set<String> hashSet = conn.zrange("known:", index, index);
                    index++;
                    if (hashSet.size() == 0)
                    {
                        break;
                    }
                    String hash = hashSet.iterator().next();
                    int prec = Integer.parseInt(hash.substring(0, hash.indexOf(':')));
                    int bprec = (int) Math.floor(prec / 60);
                    if (bprec == 0)
                    {
                        bprec = 1;
                    }
                    if ((passes % bprec) != 0)
                    {
                        continue;
                    }

                    String hkey = "count:" + hash;
                    String cutoff = String.valueOf(
                            ((System.currentTimeMillis() + timeOffset) / 1000) - sampleCount * prec);
                    ArrayList<String> samples = new ArrayList<String>(conn.hkeys(hkey));
                    Collections.sort(samples);
                    int remove = bisectRight(samples, cutoff);

                    if (remove != 0)
                    {
                        conn.hdel(hkey, samples.subList(0, remove).toArray(new String[0]));
                        if (remove == samples.size())
                        {
                            conn.watch(hkey);
                            if (conn.hlen(hkey) == 0)
                            {
                                Transaction trans = conn.multi();
                                trans.zrem("known:", hash);
                                trans.exec();
                                index--;
                            }
                            else
                            {
                                conn.unwatch();
                            }
                        }
                    }
                }

                passes++;
                long duration = Math.min(
                        (System.currentTimeMillis() + timeOffset) - start + 1000, 60000);
                try
                {
                    sleep(Math.max(60000 - duration, 1000));
                }
                catch (InterruptedException ie)
                {
                    Thread.currentThread().interrupt();
                }
            }
        }

        public int bisectRight(List<String> values, String key)
        {
            int index = Collections.binarySearch(values, key);
            return index < 0 ? Math.abs(index) - 1 : index + 1;
        }
    }

}
