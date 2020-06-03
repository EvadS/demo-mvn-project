package com.se;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import java.util.*;
public class LoadBalancer {
    private static final Map<Resource, Long>
            resourcesQueueDurationCache = new HashMap<>();
    public static long resourceCacheUpdateTime;
    private static int roundRobinLastAssignedResourceId = -1;
    public static long[] executeAllTasks(LoadBalancer.Rule rule,
                                         List<Task> tasks, List<Resource> resources,
                                         long cacheUpdateInterval,
                                         long queueAppendingInterval) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        for (Task task : tasks) {
            task.addedToTheQueue();
            getTargetResource(rule, task, resources,
                    cacheUpdateInterval).executeTask(task);
            Thread.sleep(queueAppendingInterval);
        }
        while (tasks.stream().filter(Task::isCompleted).count() !=
                tasks.size()) {
            Thread.sleep(25);
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        final long maxWaitTime = tasks.stream().max((t1, t2) ->
                Long.compare(t1.getWaitTime(), t2.getWaitTime())).get().getWaitTime();
        long avgWaitTime = 0;
        for (Task task : tasks) avgWaitTime += task.getWaitTime();
        avgWaitTime /= tasks.size();
        final long maxIdleTime = resources.stream().max((r1, r2) ->
                Long.compare(r1.getIdle(), r2.getIdle())).get().getIdle();
        long avgIdleTime = 0;
        for (Resource resource : resources) avgIdleTime +=
                resource.getIdle();
        avgIdleTime /= resources.size();
        return new long[]{duration, avgWaitTime, maxWaitTime,
                avgIdleTime, maxIdleTime};
    }
    public static Resource getTargetResource(Rule rule, Task task,
                                             List<Resource> resources, long cacheUpdateInterval) {
        switch (rule) {
            case RANDOM:
                return getResourceByRandom(resources);
            case ROUND_ROBIN:
                return getResourceByRoundRobin(resources);
            case MIN_EXECUTION:
                return getResourceByMinExecution(resources);
            case MIN_QUEUE:
                return getResourceByMinQueueSize(resources);
            case K_MEANS:
                return getResourceByKMeans(resources);
            case ADAPTIVE:
                return getResourceAdaptively(task, resources,
                        cacheUpdateInterval);
            default:
                throw new NotImplementedException();
        }
    }
    private static Resource getResourceByRandom(List<Resource>
                                                        resources) {
        return resources.get((int) (Math.random() *
                resources.size()));
    }
    private static Resource getResourceByRoundRobin(List<Resource>
                                                            resources) {
        return resources.get(++roundRobinLastAssignedResourceId <
                resources.size() ?
                roundRobinLastAssignedResourceId :
                (roundRobinLastAssignedResourceId = 0));
    }
    private static Resource getResourceByMinExecution(List<Resource>
                                                              resources) {
        Resource resourceWithMinQueueDuration = resources.get(0);
        for (Resource resource : resources) {
            if (resource == resourceWithMinQueueDuration) continue;
            if (resource.getQueueDuration() <
                    resourceWithMinQueueDuration.getQueueDuration()) {
                resourceWithMinQueueDuration = resource;
            }
        }
        return resourceWithMinQueueDuration;
    }
    private static Resource getResourceByMinQueueSize(List<Resource>
                                                              resources) {
        Resource resourceWithMinQueue = resources.get(0);
        for (Resource resource : resources) {
            if (resource == resourceWithMinQueue) continue;
            if (resource.getQueueSize() <
                    resourceWithMinQueue.getQueueSize()) {
                resourceWithMinQueue = resource;
            }
        }
        return resourceWithMinQueue;
    }
    private static Resource getResourceByKMeans(List<Resource>
                                                        resources) {
        Collections.shuffle(resources);
        Resource resourceWithMinQueue = resources.get(0);
        for (int i = 0; i < resources.size() / 2; i++) {
            Resource resource = resources.get(i);
            if (resource == resourceWithMinQueue) continue;
            if (resource.getQueueDuration() <
                    resourceWithMinQueue.getQueueDuration()) {
                resourceWithMinQueue = resource;
            }
        }
        return resourceWithMinQueue;
    }
    private static Resource getResourceAdaptively(Task task,
                                                  List<Resource> resources, long cacheUpdateInterval) {
        if (System.currentTimeMillis() - resourceCacheUpdateTime >
                cacheUpdateInterval ||
                resourceCacheUpdateTime == 0) {
            updateCache(resources);
            resourceCacheUpdateTime = System.currentTimeMillis();
        }
        Resource targetResource =
                resourcesQueueDurationCache.keySet().stream().findFirst().get();
        long targetResourceFutureQueueDuration = (long)
                (resourcesQueueDurationCache.get(targetResource) +
                        task.getDataAccessTime(targetResource) +
                        task.getExecutionTime() / targetResource.getProcessingRate());
        for (Resource resource : resourcesQueueDurationCache.keySet())
        {
            if (resource == targetResource) continue;
            long resourceFutureQueueDuration = (long)
                    (resourcesQueueDurationCache.get(resource) +
                            task.getDataAccessTime(resource) +
                            task.getExecutionTime() / resource.getProcessingRate());
            if (resourceFutureQueueDuration <
                    targetResourceFutureQueueDuration) {
                targetResource = resource;
                targetResourceFutureQueueDuration =
                        resourceFutureQueueDuration;
            }
        }
        Random random = new Random();
        resourcesQueueDurationCache.put(targetResource, (long)
                (targetResourceFutureQueueDuration * (0.75 + random.nextDouble() *
                        0.5)));
        return targetResource;
    }
    private static void updateCache(List<Resource> resources) {
        resourcesQueueDurationCache.clear();
        for (Resource resource : resources) {
            resourcesQueueDurationCache.put(resource,
                    resource.getQueueDuration());
        }
    }
    enum Rule {
        RANDOM, ROUND_ROBIN, MIN_EXECUTION, MIN_QUEUE, K_MEANS,
        ADAPTIVE
    }
}
