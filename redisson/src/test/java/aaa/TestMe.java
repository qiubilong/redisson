package aaa;

import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chenxuegui
 * @since 2025/2/12
 */
public class TestMe {

    private static final Logger log = LoggerFactory.getLogger(TestMe.class);
    private static RedissonClient redissonClient;
    private static void initRedisson(){
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:6379");
        redissonClient = Redisson.create(config);
    }


    public static void main(String[] args) throws Exception{
        initRedisson();

        Thread.sleep(3* 1000);
        new Thread(new Runnable() {
            @Override
            public void run() {
                doMain();
            }
        }).start();

        Thread.sleep( 1000);//等待获取锁成功


        new Thread(new Runnable() {
            @Override
            public void run() {
                doMain();
            }
        }).start();
        Thread.sleep(300* 1000);



        new Thread(new Runnable() {
            @Override
            public void run() {
                doMain();
            }
        }).start();
        Thread.sleep(10* 1000);


        Thread.sleep(300000* 1000);

    }

    public static void  doMain(){
        String key = "testLock";
        RLock lock = redissonClient.getLock(key);

        lock.lock();
        try {
            log.info("获得锁");
            Thread.sleep(300* 1000);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            log.info("释放锁");
            lock.unlock();

        }
    }
}
