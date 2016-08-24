package com.phunghv.cluster.demo;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.RetryOneTime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.zookeeper.KeeperException;

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
		List<Job> jobs = new ArrayList<>();
		if (args.length > 1) {
			byte[] data = Files.readAllBytes(Paths.get(args[1]));
			JSONObject config = new JSONObject(new String(data, "UTF-8"));
			JSONArray jobsArray = config.getJSONArray("jobs");
			for (int i = 0; i < jobsArray.length(); i++) {
				jobs.add(Job.fromJSON(jobsArray.getJSONObject(i), curator));
			}
			System.out.println("Loaded jobs from file");
		}
		for (String id : curator.getChildren()
				.forPath("/sampleframework/jobs")) {
			byte[] data = curator.getData()
					.forPath("/sampleframework/jobs/" + id);
			JSONObject jobJSON = new JSONObject(new String(data, "UTF-8"));
			Job job = Job.fromJSON(jobJSON, curator);
			jobs.add(job);
			System.out.println("Loaded jobs from ZK");
		}

		System.out.println("______________________________");
		System.out.println("Job size: " + jobs.size());
		for (Job j : jobs) {
			System.out.println(j.getCommand());
		}
		System.out.println("______________________________");
		FrameworkInfo.Builder frBuilder = FrameworkInfo.newBuilder().setUser("")
				.setName("Useless Remote BASH");

		try {
			byte[] curatorData = curator.getData()
					.forPath("/sampleframework/id");
			frBuilder.setId(FrameworkID.newBuilder()
					.setValue(new String(curatorData, "UTF-8")));
			System.out.println(
					"ID from curator " + new String(curatorData, "UTF-8"));
		} catch (KeeperException.NoNodeException e) {

		}

		FrameworkInfo frameworkInfo = frBuilder
				.setFailoverTimeout(60 * 60 * 24 * 7).build();

		// FrameworkInfo frameworkInfo = FrameworkInfo.newBuilder().setUser("")
		// .setName("Useless Remote BASH").build();
		Scheduler scheduler = new UselessRemoteBASH(curator, jobs);
		SchedulerDriver schedulerDriver = new MesosSchedulerDriver(scheduler,
				frameworkInfo, "zk://" + args[0] + "/mesos");
		schedulerDriver.start();
		schedulerDriver.join();
	}
}
