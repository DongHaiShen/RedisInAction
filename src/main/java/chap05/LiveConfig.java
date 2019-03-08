package chap05;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author sdh
 * @Date Created in 2019/3/8 14:26
 * @description
 */
public class LiveConfig
{
    private long lastChecked;
    private boolean underMaintenance;
    public static final Map<String, Jedis> REDIS_CONNECTIONS = new HashMap<String, Jedis>();
    private static final Map<String, Map<String, Object>> CONFIGS = new HashMap<String, Map<String, Object>>();
    private static final Map<String, Long> CHECKED = new HashMap<String, Long>();

    public static void main(String[] args) throws InterruptedException
    {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        LiveConfig liveConfig = new LiveConfig();
        liveConfig.testIsUnderMaintenance(conn);
        liveConfig.testConfig(conn);
    }

    public void testIsUnderMaintenance(Jedis conn) throws InterruptedException
    {
        System.out.println("\n----- testIsUnderMaintenance -----");
        System.out.println("Are we under maintenance (we shouldn't be)? " + isUnderMaintenance(conn));
        conn.set("is-under-maintenance", "yes");

        System.out.println("We cached this, so it should be the same: " + isUnderMaintenance(conn));
        Thread.sleep(1000);

        System.out.println("But after a sleep, it should change: " + isUnderMaintenance(conn));

        System.out.println("Cleaning up...");
        conn.del("is-under-maintenance");

        Thread.sleep(1000);
        System.out.println("Should be False again: " + isUnderMaintenance(conn));
    }

    public void testConfig(Jedis conn)
    {
        System.out.println("\n----- testConfig -----");
        System.out.println("Let's set a config and then get a connection from that config...");
        Map<String, Object> config = new HashMap<String, Object>();
        config.put("db", 15);
        setConfig(conn, "redis", "test", config);

        Jedis conn2 = redisConnection("test");
        System.out.println("We can run commands from the configured connection: " + (conn2.info() != null));
    }

    /**
     * 是否在维护
     *
     * @param conn
     * @return
     */
    public boolean isUnderMaintenance(Jedis conn)
    {
        // 距离上次检查是否已经超过1秒
        if (lastChecked < System.currentTimeMillis() - 1000)
        {
            lastChecked = System.currentTimeMillis();
            String flag = conn.get("is-under-maintenance");
            underMaintenance = "yes".equals(flag);
        }

        return underMaintenance;
    }

    /**
     * 设置配置值
     *
     * @param conn
     * @param type
     * @param component
     * @param config
     */
    public void setConfig(Jedis conn, String type, String component, Map<String, Object> config)
    {
        Gson gson = new Gson();
        conn.set("config:" + type + ':' + component, gson.toJson(config));
    }

    /**
     * 连接redis
     *
     * @param component
     * @return
     */
    public Jedis redisConnection(String component)
    {
        Jedis configConn = REDIS_CONNECTIONS.get("config");
        if (configConn == null)
        {
            configConn = new Jedis("localhost");
            configConn.select(15);
            REDIS_CONNECTIONS.put("config", configConn);
        }

        String key = "config:redis:" + component;
        Map<String, Object> oldConfig = CONFIGS.get(key);
        Map<String, Object> config = getConfig(configConn, "redis", component);

        if (!config.equals(oldConfig))
        {
            Jedis conn = new Jedis("localhost");
            if (config.containsKey("db"))
            {
                conn.select(((Double) config.get("db")).intValue());
            }
            REDIS_CONNECTIONS.put(key, conn);
        }

        return REDIS_CONNECTIONS.get(key);
    }

    /**
     * 缓存配置
     *
     * @param conn
     * @param type
     * @param component
     * @return
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getConfig(Jedis conn, String type, String component)
    {
        int wait = 1000;
        String key = "config:" + type + ':' + component;

        // 检查是否需要更新
        Long lastChecked = CHECKED.get(key);
        if (lastChecked == null || lastChecked < System.currentTimeMillis() - wait)
        {
            CHECKED.put(key, System.currentTimeMillis());

            String value = conn.get(key);
            Map<String, Object> config = null;
            if (value != null)
            {
                Gson gson = new Gson();
                config = (Map<String, Object>) gson.fromJson(value, new TypeToken<Map<String, Object>>()
                {
                }.getType());
            }
            else
            {
                config = new HashMap<String, Object>();
            }

            CONFIGS.put(key, config);
        }

        return CONFIGS.get(key);
    }

}
