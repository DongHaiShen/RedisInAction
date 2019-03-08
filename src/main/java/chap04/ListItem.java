package chap04;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Set;

/**
 * @Author sdh
 * @Date Created in 2019/3/7 20:18
 * @description
 */
public class ListItem
{
    public static void main(String[] args)
    {
        ListItem listItem = new ListItem();
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        listItem.testListItem(conn, false);
    }

    public void testListItem(Jedis conn, boolean nested)
    {
        if (!nested)
        {
            System.out.println("\n----- testListItem -----");
        }

        // 创建一个用户和一个商品
        System.out.println("We need to set up just enough state so that a user can list an item");
        String seller = "userX";
        String item = "itemX";
        conn.sadd("inventory:" + seller, item);
        Set<String> i = conn.smembers("inventory:" + seller);

        // 查看用户拥有的商品
        System.out.println("The user's inventory has:");
        for (String member : i)
        {
            System.out.println("  " + member);
        }
        assert i.size() > 0;
        System.out.println();

        // 把商品放到市场上
        System.out.println("Listing the item...");
        boolean l = listItem(conn, item, seller, 10);
        System.out.println("Listing the item succeeded? " + l);
        assert l;

        // 查看市场拥有的商品
        Set<Tuple> r = conn.zrangeWithScores("market:", 0, -1);
        System.out.println("The market contains:");
        for (Tuple tuple : r)
        {
            System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert r.size() > 0;
    }

    /**
     * 把商品放到市场上
     *
     * @param conn
     * @param itemId
     * @param sellerId
     * @param price
     * @return
     */
    public boolean listItem(Jedis conn, String itemId, String sellerId, double price)
    {
        String inventory = "inventory:" + sellerId;
        String item = itemId + '.' + sellerId;
        long end = System.currentTimeMillis() + 5000;

        while (System.currentTimeMillis() < end)
        {
            // 监视用户包裹发生的变化
            conn.watch(inventory);

            // 检查用户包裹中是否仍然拥有将要销售的商品
            if (!conn.sismember(inventory, itemId))
            {
                // 如果指定的商品不在用户的包裹里，则停止监视并返回false
                conn.unwatch();
                return false;
            }

            // 把要销售的商品添加到市场中
            Transaction trans = conn.multi();
            trans.zadd("market:", price, item);
            trans.srem(inventory, itemId);

            List<Object> results = trans.exec();

            // exec结果不为null说明执行成功，结束监视并返回true
            if (results == null)
            {
                continue;
            }
            return true;
        }
        return false;
    }
}
