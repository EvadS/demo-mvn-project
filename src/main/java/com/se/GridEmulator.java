package com.se;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
public class GridEmulator {
    public static void main(String[] args) throws Exception {
        LoadBalancer.Rule[] rules = new LoadBalancer.Rule[]{
                LoadBalancer.Rule.RANDOM,
                LoadBalancer.Rule.ROUND_ROBIN,
                LoadBalancer.Rule.MIN_EXECUTION,
                LoadBalancer.Rule.MIN_QUEUE,
                LoadBalancer.Rule.K_MEANS,
                LoadBalancer.Rule.ADAPTIVE};
        int experiments = 10;
        int amountOfTasks = 200;
        int amountOfResources = 10;
        double resourcesFluctuationRate = 0.1d;
        int minTaskDuration = 800;
        int maxTaskDuration = 1200;
        int cacheUpdateInterval = 2000;
        int maxPingTime = 10;
        int maxDataAccessTime = 10;
        int queueAppendingInterval = 200; // 100 by default
        String reportHeader = "Алгоритм, Загальний час (мс), Середній час очікування (мс), Максимальний час очікування (мс), Середній час простою (мс), Максимальний час простою (мс)";
        String fileName = String.format("./%s.csv",
                String.format("%d_%dres_%dtasks_%ddur_withoutping",
                        System.currentTimeMillis(), amountOfResources, amountOfTasks,
                        (minTaskDuration + maxTaskDuration) / 2));
        BufferedWriter bufferedWriter = new BufferedWriter(new
                OutputStreamWriter(new FileOutputStream(fileName), "UTF-8"));
        bufferedWriter.write(reportHeader);
        bufferedWriter.newLine();
        for (int i = 1; i <= experiments; i++) {
            List<Resource> resources =
                    Utils.generateResources(amountOfResources, resourcesFluctuationRate,
                            maxPingTime);
            System.out.println(resources);
            List<Task> tasks = Utils.generateTasks(amountOfTasks,
                    minTaskDuration, maxTaskDuration, resources, maxDataAccessTime);
            resources.forEach(Resource::start);
            for (final LoadBalancer.Rule rule : rules) {
                tasks.forEach(Task::reset);
                resources.forEach(Resource::reset);
                long[] results = LoadBalancer.executeAllTasks(rule,
                        tasks, resources, cacheUpdateInterval, queueAppendingInterval);
                String resultString = rule.toString() + ", " +
                        results[0] + ", " + results[1] + ", " + results[2] + ", " + results[3]
                        + ", " + results[4];
                bufferedWriter.write(resultString);
                bufferedWriter.newLine();
                System.out.println(String.format("Experiment %d/%d completed with results: %s", i, experiments, resultString));
            }
            resources.forEach(Resource::shouldBeTerminated);
        }
        bufferedWriter.close();
    }
}