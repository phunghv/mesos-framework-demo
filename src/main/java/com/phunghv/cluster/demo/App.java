package com.phunghv.cluster.demo;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.RetryOneTime;
import org.apache.http.impl.io.SocketOutputBuffer;
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

public class App {
	final static Logger logger = LogManager.getLogger(App.class);

	public static void main(String[] args) throws Exception {
		// using cuartor elect master
		CuratorFramework curator = CuratorFrameworkFactory.newClient(args[0],
				new RetryOneTime(1000));
		curator.start();
		LeaderLatch leaderLatch = new LeaderLatch(curator,
				ZookeeperContanst.FW_LEADER_PATH);
		leaderLatch.start();
		leaderLatch.await();

		// Load list jobs
		List<Job> jobs = new ArrayList<>();

		// Load job from configuration file
		if (args.length > 1) {
			byte[] data = Files.readAllBytes(Paths.get(args[1]));
			JSONObject config = new JSONObject(new String(data, "UTF-8"));
			JSONArray jobsArray = config.getJSONArray("jobs");
			for (int i = 0; i < jobsArray.length(); i++) {
				Job current = Job.fromJSON(jobsArray.getJSONObject(i));
				current.setCurator(curator);
				jobs.add(current);
			}
			logger.info("Load job file :{} with size = {}", args[1],
					jobsArray.length());
		}
		// load job from curator
		try {
			List<String> listJobs = curator.getChildren()
					.forPath(ZookeeperContanst.FW_JOB_PATH);
			for (String id : listJobs) {
				System.out.println("|" + id + "|");
			}
			logger.info("size jobs  {}", listJobs.size());
			for (String id : listJobs) {
				logger.info("job in zookeeper : {}", id);
				byte[] data = curator.getData()
						.forPath(ZookeeperContanst.FW_JOB_PATH + "/" + id);
				JSONObject jobJSON = new JSONObject(new String(data, "UTF-8"));
				Job job = Job.fromJSON(jobJSON, curator);
				jobs.add(job);
				System.out.println("Loaded jobs from ZK");
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Cannot load jobs from ZK");
		}
		for (Job j : jobs) {
			logger.info("|{}|", j.getCommand());
		}
		FrameworkInfo.Builder frBuilder = FrameworkInfo.newBuilder().setUser("")
				.setName("UselessRemoteBASH");
		try {
			byte[] curatorData = curator.getData()
					.forPath(ZookeeperContanst.FW_ID_PATH);
			frBuilder.setId(FrameworkID.newBuilder()
					.setValue(new String(curatorData, "UTF-8")));
			System.out.println(
					"ID from curator " + new String(curatorData, "UTF-8"));
		} catch (KeeperException.NoNodeException e) {
			System.out.println("no id stored on zk, allow Mesos to assign it");
		}

		FrameworkInfo frameworkInfo = frBuilder
				.setFailoverTimeout(60 * 60 * 24 * 7).build();

		Scheduler scheduler = new UselessRemoteBASH(curator, jobs);
		SchedulerDriver schedulerDriver = new MesosSchedulerDriver(scheduler,
				frameworkInfo, "zk://" + args[0] + "/mesos");
		schedulerDriver.start();
		schedulerDriver.join();
	}
}
