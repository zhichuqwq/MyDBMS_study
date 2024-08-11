package firsttry.luhewen.db.backed.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import firsttry.luhewen.db.commom.Error;
/**
 * 该类实现了引用技术策略的缓存
 * @param <T>
 */
public abstract class AbstractCache<T> {
    private HashMap<Long, T> cache;                 //实际缓存数据
    private HashMap<Long, Integer> references;      //缓存中数据引用的数量
    private HashMap<Long, Boolean> getting;         //是否有线程在获取缓存数据
    private Lock lock;
    private int maxCacheSize;                       //缓存允许的最大资源数
    private int cot = 0;                            //缓存内资源数

    public AbstractCache(int maxCacheSize){
        this.maxCacheSize = maxCacheSize;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 获取资源
     * @param key
     */
    protected T get(long key)throws Exception{
        while(true){
            lock.lock();
            if(getting.containsKey(key)){           //有其他线程获取该资源
                lock.unlock();
                try{
                    Thread.sleep(1);
                }catch (InterruptedException e){
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if(cache.containsKey(key)){             //资源存在于缓存中
                references.merge(key, 1, Integer::sum);
                lock.unlock();
                return cache.get(key);
            }

            //尝试获取资源
            if(maxCacheSize < 0 || cot == maxCacheSize){
                lock.unlock();
                throw Error.CacheFullException;
            }
            cot++;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        T obj = null;
        try{
            obj = getForCache(key);
        }catch(Exception e){
            lock.lock();
            getting.put(key, false);
            cot--;
            lock.unlock();
            throw e;
        }

        lock.lock();
        getting.put(key, false);
        references.put(key, 1);
        cache.put(key, obj);
        lock.unlock();
        return obj;
    }

    /**
     * 释放一个缓存资源
     */
    protected void release(long key){
        lock.lock();
        try{
            int ref = references.get(key);
            if(ref == 1){
                T obj = cache.get(key);
                releaseForCache(obj);
                cache.remove(key);
                references.remove(key);
                getting.remove(key);
                cot--;
            }else{
                references.put(key, ref - 1);
            }
        }finally{
            lock.unlock();
        }
    }

    /**
     * 清空缓存并将缓存中所有资源写回
     */
    protected void close(){
        lock.lock();
        try{
            Set<Long> keys = cache.keySet();
            for(long key : keys){
                T obj = cache.get(key);
                releaseForCache(obj);
            }
            getting.clear();
            references.clear();
            cache.clear();
            cot = 0;
        }finally {
            lock.unlock();
        }
    }

    /**
     * 资源不在缓存中时，获取资源
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 资源驱逐时的写回操作
     */
    protected abstract void releaseForCache(T obj);
}
