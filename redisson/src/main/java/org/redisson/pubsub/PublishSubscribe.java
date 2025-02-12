/**
 * Copyright (c) 2013-2022 Nikita Koksharov
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

import org.redisson.PubSubEntry;
import org.redisson.client.BaseRedisPubSubListener;
import org.redisson.client.ChannelName;
import org.redisson.client.RedisPubSubListener;
import org.redisson.client.codec.LongCodec;
import org.redisson.misc.AsyncSemaphore;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 
 * @author Nikita Koksharov
 *
 */
abstract class PublishSubscribe<E extends PubSubEntry<E>> {

    private final ConcurrentMap<String, E> entries = new ConcurrentHashMap<>();
    private final PublishSubscribeService service;

    PublishSubscribe(PublishSubscribeService service) {
        super();
        this.service = service;
    }

    public void unsubscribe(E entry, String entryName, String channelName) {
        ChannelName cn = new ChannelName(channelName);
        AsyncSemaphore semaphore = service.getSemaphore(cn);
        semaphore.acquire().thenAccept(c -> {
            if (entry.release() == 0) {/* 线程数减1 */
                entries.remove(entryName);/* 线程数为0时，取消订阅解锁消息channel */
                service.unsubscribeLocked(cn)
                        .whenComplete((r, e) -> {
                            semaphore.release();
                        });
            } else {
                semaphore.release();
            }
        });
    }

    public void timeout(CompletableFuture<?> promise) {
        service.timeout(promise);
    }

    public void timeout(CompletableFuture<?> promise, long timeout) {
        service.timeout(promise, timeout);
    }
    /*  entryName = 锁节点名字 = RedissonClientId + 锁名字  */
    public CompletableFuture<E> subscribe(String entryName, String channelName) {/* 订阅解锁消息channel */
        AsyncSemaphore semaphore = service.getSemaphore(new ChannelName(channelName));//限流器，维护任务串行执行
        CompletableFuture<E> newPromise = new CompletableFuture<>();

        semaphore.acquire().thenAccept(c -> {
            if (newPromise.isDone()) {
                semaphore.release();//执行完毕，发放令牌，驱动执行下个任务
                return;
            }

            E entry = entries.get(entryName);
            if (entry != null) {//非首个订阅解锁消息线程，不需要再次订阅解锁channel消息
                entry.acquire();//等待线程加1
                semaphore.release();//发放令牌
                entry.getPromise().whenComplete((r, e) -> {
                    if (e != null) {
                        newPromise.completeExceptionally(e);
                        return;
                    }
                    newPromise.complete(r);
                });
                return;
            }

            E value = createEntry(newPromise);/* 首个订阅解锁消息线程，创建锁等待节点对象 */
            value.acquire();//等待线程加1

            E oldValue = entries.putIfAbsent(entryName, value);
            if (oldValue != null) {
                oldValue.acquire();
                semaphore.release();
                oldValue.getPromise().whenComplete((r, e) -> {
                    if (e != null) {
                        newPromise.completeExceptionally(e);
                        return;
                    }
                    newPromise.complete(r);
                });
                return;
            }
            /* 首个订阅线程 --> 创建解锁消息监听器 --> 利用redis channel机制订阅监听解锁消息 --> 唤醒阻塞等待线程 --> 重新竞争分布式锁  */
            RedisPubSubListener<Object> listener = createListener(channelName, value);
            CompletableFuture<PubSubConnectionEntry> s = service.subscribeNoTimeout(LongCodec.INSTANCE, channelName, semaphore, listener);
            newPromise.whenComplete((r, e) -> {
                if (e != null) {
                    s.completeExceptionally(e);
                }
            });
            s.whenComplete((r, e) -> {
                if (e != null) {
                    entries.remove(entryName);
                    value.getPromise().completeExceptionally(e);
                    return;
                }
                value.getPromise().complete(value);
            });

        });

        return newPromise;
    }

    protected abstract E createEntry(CompletableFuture<E> newPromise);

    protected abstract void onMessage(E value, Long message);

    private RedisPubSubListener<Object> createListener(String channelName, E value) {
        RedisPubSubListener<Object> listener = new BaseRedisPubSubListener() {

            @Override
            public void onMessage(CharSequence channel, Object message) {
                if (!channelName.equals(channel.toString())) {
                    return;
                }

                PublishSubscribe.this.onMessage(value, (Long) message);/* 收到订阅消息 */
            }
        };
        return listener;
    }

}
