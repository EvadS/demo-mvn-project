package com.se;


import java.util.*;

public class Utils {
    static List<Task> generateTasks(int amount, int minTaskDuration,
                                    int maxTaskDuration, List<Resource> resources,
                                    int maxAccessTime) {
        ArrayList<Task> tasks = new ArrayList<>(amount);
        Random random = new Random();
        for (int i = 0; i < amount; i++) {
            Task task = new Task(random.nextInt(maxTaskDuration -
                    minTaskDuration) + minTaskDuration);
            Map<Resource, Integer> dataAccessTime = new HashMap<>();
            for (Resource resource : resources) {
                dataAccessTime.put(resource,
                        random.nextInt(maxAccessTime));
            }
            task.setResourceDataAccessTime(dataAccessTime);
            tasks.add(task);
        }
        return tasks;
    }

    static List<Resource> generateResources(int amount, double
            fluctuationRate, int maxPingTime) {
        ArrayList<Resource> resources = new ArrayList<>(amount);
        Random random = new Random();
        for (int i = 0; i < amount; i++) {
            Resource resource = new Resource(1 - fluctuationRate + 2f
                    * fluctuationRate * random.nextDouble(), random.nextInt(maxPingTime));
            resources.add(resource);
        }
        LoadBalancer.resourceCacheUpdateTime = 0;
        return resources;
    }
}