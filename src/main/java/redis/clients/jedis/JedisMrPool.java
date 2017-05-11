package redis.clients.jedis;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Created by wenshiliang on 2017/5/11.
 */
public class JedisMrPool extends JedisPoolAbstract {
    protected GenericObjectPoolConfig poolConfig;

    protected int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
    protected int soTimeout = Protocol.DEFAULT_TIMEOUT;

    protected String password;

    protected int database = Protocol.DEFAULT_DATABASE;

    protected String clientName;


    protected Logger log = LoggerFactory.getLogger(getClass());

    private volatile JedisFactory factory;
    private volatile HostAndPort currentHostMaster;

    private String redisInstanceName;//redis 实例名称
    private MrRedisEndPoint mrRedisEndPoint;//mr控制地址


    public JedisMrPool(MrRedisEndPoint mrRedisEndPoint, String redisInstanceName,
                       final GenericObjectPoolConfig poolConfig) {
        this(mrRedisEndPoint, redisInstanceName, poolConfig, Protocol.DEFAULT_TIMEOUT, Protocol.DEFAULT_TIMEOUT);
    }

    public JedisMrPool(MrRedisEndPoint mrRedisEndPoint, String redisInstanceName,
                       final GenericObjectPoolConfig poolConfig, final int connectionTimeout) {
        this(mrRedisEndPoint,redisInstanceName,poolConfig,connectionTimeout, Protocol.DEFAULT_TIMEOUT);
    }

    public JedisMrPool(MrRedisEndPoint mrRedisEndPoint, String redisInstanceName,
                       final GenericObjectPoolConfig poolConfig, final int connectionTimeout, final int soTimeout) {
        this.mrRedisEndPoint = mrRedisEndPoint;
        this.redisInstanceName = redisInstanceName;
        this.poolConfig = poolConfig;
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;


        init();

    }
    public synchronized void init(){
        HostAndPort master = initMaster(mrRedisEndPoint, redisInstanceName);
        initPool(master);
    }

    private  HostAndPort initMaster(MrRedisEndPoint mrRedisEndPoint, String redisInstanceName) {
        HostAndPort master = mrRedisEndPoint.getMaster(redisInstanceName);

        return master;
    }

    private  void initPool(HostAndPort master) {
        if (!master.equals(currentHostMaster)) {
            currentHostMaster = master;
            if (factory == null) {
                factory = new JedisFactory(master.getHost(), master.getPort(), connectionTimeout,
                        soTimeout, password, database, clientName, false, null, null, null);
                initPool(poolConfig, factory);
            } else {
                factory.setHostAndPort(currentHostMaster);
                internalPool.clear();
            }

            log.info("Created JedisPool to master at " + master);
        }
    }

    public void destroy() {
        super.destroy();
    }


    @Override
    public Jedis getResource() {
        while (true) {
            Jedis jedis = super.getResource();
            jedis.setDataSource(this);

            // get a reference because it can change concurrently
            final HostAndPort master = currentHostMaster;
            final HostAndPort connection = new HostAndPort(jedis.getClient().getHost(), jedis.getClient()
                    .getPort());

            if (master.equals(connection)) {
                // connected to the correct master
                JedisProxy jedisProxy = new JedisProxy(jedis,this);

                return jedisProxy.create();
            } else {
                returnBrokenResource(jedis);
            }
        }
    }


    protected static class JedisProxy implements MethodInterceptor {
        private static List<String> interceptMethods;

        {
            interceptMethods = new ArrayList<>();
            Method[] methods = JedisCommands.class.getMethods();
            for (Method method : methods) {
                interceptMethods.add(method.getName());
            }
        }

        private static Enhancer enhancer = new Enhancer();
        private Jedis jedis;
        private JedisMrPool jedisMrPool;


        public JedisProxy(Jedis jedis, JedisMrPool jedisMrPool) {
            this.jedis = jedis;
            this.jedisMrPool = jedisMrPool;
        }

        public Jedis create(){
            enhancer.setSuperclass(Jedis.class);
            enhancer.setCallback(this);
            enhancer.setInterfaces(new Class[]{JedisCommands.class});
            return (Jedis) enhancer.create();
        }


        @Override
        public Object intercept(Object o, Method method, Object[] args, MethodProxy methodProxy) throws Throwable {
            if (Object.class.equals(method.getDeclaringClass())) {
                try {
                    // 诸如hashCode()、toString()、equals()等方法，将target指向当前对象this
                    return method.invoke(this, args);
                } catch (Throwable t) {
                    throw new Exception("Object method invoke exception!");
                }
            }else if(interceptMethods.contains(method.getName())){
                //判断是JedisCommands 接口中的方法再拦截.
                //加入拦截,检测到特定异常,请求mr接口判断redis实例存活
                try{
                    return methodProxy.invoke(jedis, args);
                }catch (JedisConnectionException e){
                    jedisMrPool.init();
                    throw e;
                }
            }else{
                return methodProxy.invoke(jedis, args);
            }
        }
    }




    public static void main(String[] args) throws NoSuchMethodException {



        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(10);
        jedisPoolConfig.setMaxWaitMillis(1000);
        MrRedisEndPoint point = new MrRedisEndPoint("http://192.168.1.80:5656");
        JedisMrPool pool = new JedisMrPool(point,"test1",jedisPoolConfig);

        Jedis jedis = pool.getResource();

        try{
            if (true) {
                for (; ; ) {
                    jedis.set("testkey", "testvalue");
                    String value = jedis.get("testkey");
                    System.out.println(value);
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        }catch (Exception e){
            e.printStackTrace();
            Jedis j2 = pool.getResource();
            System.out.println(j2.get("testkey"));
            j2.close();
        }

        try{
            jedis.close();
        }catch (Exception e){
            e.printStackTrace();
        }






    }
}
