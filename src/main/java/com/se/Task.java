package com.se;

import java.util.HashMap;
import java.util.Map;
public class Task {
    private static int nextId;
    private int id;
    private long executionTime;
    private Map<Resource, Integer> resourceDataAccessTime = new
            HashMap<>();
    private long startTime;
    private long endTime;
    private long waitTime;
    public Task(int executionTime) {
        id = nextId++;
        this.executionTime = executionTime;
    }
    public void addedToTheQueue() {
        startTime = System.currentTimeMillis();
    }
    public long getWaitTime() {
        return waitTime;
    }
    public boolean isCompleted() {
        return endTime != 0;
    }
    public long getExecutionTime() {
        return executionTime;
    }
    public void execute(Resource resource) {
        if (startTime == 0) throw new IllegalStateException("Task should be added to the queue first!");
        waitTime = System.currentTimeMillis() - startTime;
        try {
            Thread.sleep(getDataAccessTime(resource) + (long)
                    (executionTime / resource.getProcessingRate()));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        endTime = System.currentTimeMillis();
    }
    @Override
    public String toString() {
        return "Task[" + id + "]:{" + executionTime + "}";
    }
    public void reset() {
        startTime = 0;
        endTime = 0;
        waitTime = 0;
    }
    public int getDataAccessTime(Resource resource) {
        if (resourceDataAccessTime != null && resource != null) {
            return resourceDataAccessTime.get(resource);
        } else return 0;
    }
    public void setResourceDataAccessTime(Map<Resource, Integer>
                                                  resourceDataAccessTime) {
        this.resourceDataAccessTime = resourceDataAccessTime;
    }
}