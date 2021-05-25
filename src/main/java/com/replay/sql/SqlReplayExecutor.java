package com.replay.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class SqlReplayExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SqlReplayExecutor.class);
    private static final int MAX_THREAD_NUM = 800;
    private static final Map<Long, Worker> worksMap = new ConcurrentHashMap<>(64);
    private static final AtomicLong THREAD_COUNT = new AtomicLong(0L);

    private static volatile int minQueueSize =  0;
    private static volatile long minSizeThreadId =  0;

    public Worker addWorker(Runnable firstTask) {
        Worker w = new Worker(firstTask);
        final Thread t = w.thread;
        if (t != null) {
            if (t.isAlive()) {
                throw new IllegalThreadStateException();
            }
            t.start();
        }
        return w;
    }


    private class Worker implements Runnable {
        private LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue(20000);
        private Thread thread;
        private Runnable firstTask;

        Worker(Runnable firstTask) {
            this.firstTask = firstTask;
            this.thread = new Thread(this, "sql-replay-worker-" + THREAD_COUNT.addAndGet(1));
        }


        @Override
        public void run() {
            runWorker(this);
        }

        final void runWorker(Worker w) {
            Runnable task = w.firstTask;
            w.firstTask = null;
            while (task != null || (task = getTask()) != null) {
                try {
                    try {
                        task.run();
                    } catch (Throwable x) {
                        throw new Error(x);
                    }
                } finally {
                    task = null;
                }
            }
        }

        private Runnable getTask() {
            for (; ; ) {
                try {
                    return workQueue.take();
                } catch (InterruptedException e) {
                    logger.error("从队列中获取任务失败异常，循环获取", e.getMessage());
                }
            }
        }

        public void offerTask(Runnable runnable) {
            boolean result = workQueue.offer(runnable);
            if (!result) {
                logger.warn("___重放队列已满___________");
            }
        }

        public int getQueueSize() {
            return workQueue.size();
        }

    }

    public void execute(Runnable command, Long threadId) {
        if (command == null) {
            throw new NullPointerException();
        }
        if (worksMap.containsKey(threadId)) {
            worksMap.get(threadId).offerTask(command);
            int queueSize = worksMap.get(threadId).getQueueSize();
            if (minQueueSize > queueSize || minSizeThreadId == threadId) {
                minSizeThreadId = threadId;
                minQueueSize = queueSize;
            }
        } else {
            if (worksMap.size() > MAX_THREAD_NUM) {
                threadId = minSizeThreadId;
            }
            if (worksMap.containsKey(threadId)) {
                worksMap.get(threadId).offerTask(command);
                int queueSize = worksMap.get(threadId).getQueueSize();
                if (minQueueSize > queueSize || minSizeThreadId == threadId) {
                    minSizeThreadId = threadId;
                    minQueueSize = queueSize;
                }
            } else {
                if (minSizeThreadId == 0) {
                    minSizeThreadId = threadId;
                }
                worksMap.put(threadId, addWorker(command));
            }
        }
    }
}
