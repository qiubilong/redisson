/**
 * Copyright 2018 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.pubsub;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.redisson.PubSubEntry;
import org.redisson.api.RFuture;
import org.redisson.client.BaseRedisPubSubListener;
import org.redisson.client.RedisPubSubListener;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.pubsub.PubSubType;
import org.redisson.connection.ConnectionManager;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;
import org.redisson.misc.TransferListener;

import io.netty.util.internal.PlatformDependent;

/**
 * 
 * @author Nikita Koksharov
 *
 */
abstract class PublishSubscribe<E extends PubSubEntry<E>> {

    private final ConcurrentMap<String, E> entries = PlatformDependent.newConcurrentHashMap();
    /* 取消订阅解锁消息 */
    public void unsubscribe(final E entry, final String entryName, final String channelName, final PublishSubscribeService subscribeService) {
        final AsyncSemaphore semaphore = subscribeService.getSemaphore(channelName);
        semaphore.acquire(new Runnable() {
            @Override
            public void run() {
                if (entry.release() == 0) {/* 锁等待线程数减1 */
                    // just an assertion
                    boolean removed = entries.remove(entryName) == entry;
                    if (!removed) {
                        throw new IllegalStateException();
                    }
                    subscribeService.unsubscribe(channelName, semaphore);  /* 没有锁等待者后，取消channel订阅 */
                } else {
                    semaphore.release(); /* 限流令牌 */
                }
            }
        });

    }

    public E getEntry(String entryName) {
        return entries.get(entryName);
    }
    /* 锁等待节点entryName == RedissonClient实例ID + lockKey ;      锁释放消息频道channelName = redisson_countdownlatch__channel__{lockKey}      */
    public RFuture<E> subscribe(final String entryName, final String channelName, final PublishSubscribeService subscribeService) {
        final AtomicReference<Runnable> listenerHolder = new AtomicReference<Runnable>();
        final AsyncSemaphore semaphore = subscribeService.getSemaphore(channelName);/* 订阅操作限流器 */
        final RPromise<E> newPromise = new RedissonPromise<E>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return semaphore.remove(listenerHolder.get());
            }
        };

        Runnable listener = new Runnable() {

            @Override
            public void run() {
                E entry = entries.get(entryName);
                if (entry != null) {/* 非第一个等待者 */
                    entry.aquire();//等待线程加1
                    semaphore.release();//限流令牌+1
                    entry.getPromise().addListener(new TransferListener<E>(newPromise));
                    return;
                }
                
                E value = createEntry(newPromise);/* 创建分布式锁等待节点 RedissonLockEntry ，每一个锁最多一个*/
                value.aquire();//等待线程加1
                
                E oldValue = entries.putIfAbsent(entryName, value);
                if (oldValue != null) {
                    oldValue.aquire();
                    semaphore.release();
                    oldValue.getPromise().addListener(new TransferListener<E>(newPromise));
                    return;
                }
                /* 创建监听器 --> 注册订阅解锁消息监听器 -->解锁时唤醒获取锁等待线程 */
                RedisPubSubListener<Object> listener = createListener(channelName, value);
                subscribeService.subscribe(LongCodec.INSTANCE, channelName, semaphore, listener);//操作成功后 semaphore.release();
            }
        };
        semaphore.acquire(listener);/* 限流执行listener */
        listenerHolder.set(listener);
        
        return newPromise;
    }

    protected abstract E createEntry(RPromise<E> newPromise);

    protected abstract void onMessage(E value, Long message);

    private RedisPubSubListener<Object> createListener(final String channelName, final E value) {
        RedisPubSubListener<Object> listener = new BaseRedisPubSubListener() {

            @Override
            public void onMessage(String channel, Object message) { /* 订阅锁释放消息  */
                if (!channelName.equals(channel)) {
                    return;
                }

                PublishSubscribe.this.onMessage(value, (Long)message);/* 分布式锁释放消息 */
            }

            @Override
            public boolean onStatus(PubSubType type, String channel) {
                if (!channelName.equals(channel)) {
                    return false;
                }

                if (type == PubSubType.SUBSCRIBE) {
                    value.getPromise().trySuccess(value);
                    return true;
                }
                return false;
            }

        };
        return listener;
    }

}
