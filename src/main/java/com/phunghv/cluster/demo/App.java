package com.phunghv.cluster.demo;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyException;
import java.util.ArrayList;
import java.util.List;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.RetryOneTime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.FrameworkInfo;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

/**
 * Hello world!
 *
 */
public class App {
	final static Logger logger = LogManager.getLogger(App.class);

	public static void main(String[] args) throws Exception {
		logger.info("Start main function");

		CuratorFramework curator = CuratorFrameworkFactory.newClient(args[0],
				new RetryOneTime(1000));
		curator.start();

		LeaderLatch leaderLatch = new LeaderLatch(curator,
				"/sampleframework/leader");
		leaderLatch.start();
		leaderLatch.await();
		// Load list jobs

		byte[] data = Files.readAllBytes(Paths.get(args[1]));
		JSONObject config = new JSONObject(new String(data, "UTF-8"));
		JSONArray jobsArray = config.getJSONArray("jobs");
		List<Job> jobs = new ArrayList<>();
		for (int i = 0; i < jobsArray.length(); i++) {
			jobs.add(Job.fromJSON(jobsArray.getJSONObject(i)));
		}
		System.out.println("______________________________");
		System.out.println("Job size: " + jobs.size());
		for (Job j : jobs) {
			System.out.println(j.getCommand());
		}
		System.out.println("______________________________");
		FrameworkInfo.Builder frBuilder = FrameworkInfo.newBuilder().setUser("")
				.setName("Useless Remote BASH");

		byte[] curatorData = curator.getData().forPath("/sampleframework/id");
		// frBuilder.setId(new String(curatorData,"UTF-8"));

		FrameworkInfo frameworkInfo = FrameworkInfo.newBuilder().setUser("")
				.setName("Useless Remote BASH").build();
		Scheduler scheduler = new UselessRemoteBASH(jobs);
		SchedulerDriver schedulerDriver = new MesosSchedulerDriver(scheduler,
				frameworkInfo, "zk://" + args[0] + "/mesos");
		schedulerDriver.start();
		schedulerDriver.join();
	}
}
