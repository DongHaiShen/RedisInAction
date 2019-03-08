package chap04;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author sdh
 * @Date Created in 2019/3/7 20:32
 * @description
 */
public class PurchaseItem
{
    public static void main(String[] args)
    {
        PurchaseItem purchaseItem = new PurchaseItem();
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        purchaseItem.testPurchaseItem(conn);
    }

    public void testPurchaseItem(Jedis conn)
    {
        // 商品放入市场
        System.out.println("\n----- testPurchaseItem -----");
        ListItem listItem = new ListItem();
        listItem.testListItem(conn, true);
        System.out.println();

        // 创建一个用户和他拥有的资金
        System.out.println("We need to set up just enough state so a user can buy an item");
        conn.hset("users:userY", "funds", "125");

        // 查看用户拥有的资金
        Map<String, String> r = conn.hgetAll("users:userY");
        System.out.println("The user has some money:");
        for (Map.Entry<String, String> entry : r.entrySet())
        {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        assert r.size() > 0;
        assert r.get("funds") != null;
        System.out.println();

        // 购买一个商品
        System.out.println("Let's purchase an item");
        boolean p = purchaseItem(conn, "userY", "itemX", "userX", 10);
        System.out.println("Purchasing an item succeeded? " + p);
        assert p;

        // 查看用户剩余资金
        r = conn.hgetAll("users:userY");
        System.out.println("Their money is now:");
        for (Map.Entry<String, String> entry : r.entrySet())
        {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        assert r.size() > 0;

        // 查看用户包裹内的商品
        String buyer = "userY";
        Set<String> i = conn.smembers("inventory:" + buyer);
        System.out.println("Their inventory is now:");
        for (String member : i)
        {
            System.out.println("  " + member);
        }
        assert i.size() > 0;
        assert i.contains("itemX");

        // 市场应当不再拥有商品
        assert conn.zscore("market:", "itemX.userX") == null;
    }

    /**
     * 从市场上购买商品
     *
     * @param conn
     * @param buyerId
     * @param itemId
     * @param sellerId
     * @param lprice
     * @return
     */
    public boolean purchaseItem(Jedis conn, String buyerId, String itemId, String sellerId, double lprice)
    {
        String buyer = "users:" + buyerId;
        String seller = "users:" + sellerId;
        String item = itemId + '.' + sellerId;
        String inventory = "inventory:" + buyerId;
        long end = System.currentTimeMillis() + 10000;

        while (System.currentTimeMillis() < end)
        {
            // 监视市场和买家信息
            conn.watch("market:", buyer);

            double price = conn.zscore("market:", item);
            double funds = Double.parseDouble(conn.hget(buyer, "funds"));

            // 检测买家想要购买的商品价格是否发生变化
            // 以及买家是否有足够的钱来购买
            if (price != lprice || price > funds)
            {
                conn.unwatch();
                return false;
            }

            Transaction trans = conn.multi();

            // 先把买家支付的钱加给卖家
            trans.hincrBy(seller, "funds", (int) price);
            trans.hincrBy(buyer, "funds", (int) -price);

            // 再把市场上的商品移交给买家
            trans.sadd(inventory, itemId);
            trans.zrem("market:", item);

            // exec结果不为null说明执行成功，结束监视并返回true
            List<Object> results = trans.exec();
            if (results == null)
            {
                continue;
            }
            return true;
        }

        return false;
    }
}
