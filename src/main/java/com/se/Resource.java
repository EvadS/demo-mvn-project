package com.se;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
public class Resource extends Thread {
    private static int nextId;
    private final Queue<Task> queue;
    private int id;
    private boolean shouldBeTerminated;
    private double processingRate;
    private long pingTime;
    private long idleStartTime;
    private long idle;
    public Resource(double processingRate, long pingTime) {
        id = nextId++;
        queue = new LinkedBlockingQueue<>();
        shouldBeTerminated = false;
        this.processingRate = processingRate;
        this.pingTime = pingTime;
    }
    public void executeTask(Task task) {
        sleep();
        synchronized (queue) {
            queue.add(task);
        }
    }
    @Override
    public void run() {
        idleStartTime = System.currentTimeMillis();
        while (!shouldBeTerminated) {
            Task task;
            synchronized (queue) {
                task = queue.peek();
            }
            if (task != null) {
                if (idleStartTime != 0) {
                    idle += (System.currentTimeMillis() -
                            idleStartTime);
                    idleStartTime = 0;
                }
                task.execute(this);
                synchronized (queue) {
                    queue.remove(task);
                    if (queue.isEmpty()) {
                        idleStartTime = System.currentTimeMillis();
                    }
                }
            }
        }
    }
    public boolean isBusy() {
        sleep();
        return queue.size() != 0;
    }
    public int getQueueSize() {
        sleep();
        return queue.size();
    }
    public long getIdle() {
        return idle;
    }
    public void shouldBeTerminated() {
        shouldBeTerminated = true;
    }
    public long getQueueDuration() {
        sleep();
        long timeLeft = 0;
        for (Task task : queue) {
            timeLeft += task.getExecutionTime();
        }
        return timeLeft;
    }
    public double getProcessingRate() {
        return processingRate;
    }
    private void sleep() {
        if (pingTime != 0) {
            try {
                Thread.sleep(pingTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    @Override
    public String toString() {
        return String.format("Resource[%d]: {perf=%f, pingTime=%d}",
                id, processingRate, pingTime);
    }
    public void reset() {
        idle = 0;
        idleStartTime = 0;
    }
}