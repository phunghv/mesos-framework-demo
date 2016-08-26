package com.phunghv.cluster.demo;

import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Value;
import org.apache.zookeeper.KeeperException;
import org.json.*;

public class Job {
	private double cpus;
	private double mem;
	private String command;
	private boolean submitted;
	private JobState status;
	private String id;
	private int retries;
	private CuratorFramework curator;
	private SlaveID slaveId;
	private String newly;
	private String failover;
	private boolean isFailed = false;

	final static Logger logger = LogManager.getLogger(Job.class);

	private Job() {
		this.submitted = false;
		this.status = JobState.PENDING;
		this.id = UUID.randomUUID().toString();
		this.retries = 10;
		this.newly = null;
		this.failover = null;
	}

	private Job(CuratorFramework curator) {
		this.submitted = false;
		this.curator = curator;
		this.status = JobState.PENDING;
		this.id = UUID.randomUUID().toString();
		this.retries = 10;
		this.newly = null;
		this.failover = null;
	}

	public void launch() {
		logger.info("launch job");
		this.status = JobState.STAGING;
		saveState();
	}

	public void started() {
		logger.info("job started");
		this.status = JobState.RUNNING;
		saveState();
	}

	public void succeed() {
		logger.info("job succeed");
		this.status = JobState.SUCCESSFUL;
		saveState();
	}

	public void fail() {
		logger.info("retry job ");
		this.isFailed = true; // flag task is fail
		System.out.println("Retry count " + (10 - this.retries));
		if (this.retries == 0) {
			logger.info("job failed");
			this.status = JobState.FAILED;
		} else {
			this.retries--;
			logger.info("counter --- retry job count {}", this.retries);
			this.status = JobState.PENDING;
			this.submitted = false;
		}
		saveState();
	}

	public TaskInfo makeTask(SlaveID targetSlave) {
		logger.info("make task on {}", targetSlave.toString());
		// UUID uuid = UUID.randomUUID();
		TaskID id = TaskID.newBuilder().setValue(this.id).build();
		this.setSlaveId(targetSlave);
		// this.setId(id.getValue());
		return TaskInfo.newBuilder().setName("phunghv.task_" + id.getValue())
				.setTaskId(id)
				.addResources(
						Resource.newBuilder().setName("cpus")
								.setType(Value.Type.SCALAR)
								.setScalar(Value.Scalar.newBuilder()
										.setValue(this.cpus)))
				.addResources(
						Resource.newBuilder().setName("mem")
								.setType(Value.Type.SCALAR)
								.setScalar(Value.Scalar.newBuilder()
										.setValue(this.mem)))
				.setCommand(CommandInfo.newBuilder().setValue(this.command))
				.setSlaveId(targetSlave).build();
	}

	public static Job fromJSON(JSONObject json) throws JSONException {
		Job job = new Job();
		job.setCpus(json.getDouble("cpus"));
		job.setMem(json.getDouble("mem"));
		job.setCommand(json.getString("command"));
		job.setCpus(json.getDouble("cpus"));
		if (json.has("newly")) {
			job.setNewly(json.getString("newly"));
		}
		if (json.has("failover")) {
			job.setFailover(json.getString("failover"));
		}
		return job;
	}

	public static Job fromJSON(JSONObject json, CuratorFramework curator)
			throws JSONException {
		Job job = new Job();
		job.setId(json.getString("id"));
		logger.info("create job from json with curator :{}",
				json.getString("status"));
		switch (json.getString("status")) {
		case "RUNNING":
			job.setStatus(JobState.RUNNING);
			job.setSubmitted(true);
			break;
		case "FAILED":
			job.setStatus(JobState.FAILED);
			break;
		case "SUCCESSFUL":
			job.setStatus(JobState.SUCCESSFUL);
			job.setSubmitted(true);
			break;
		case "PENDING":
			job.setStatus(JobState.PENDING);

			break;
		default:
			job.setStatus(JobState.FAILED);
			break;
		}
		job.setCpus(json.getDouble("cpus"));
		if (json.has("newly")) {
			job.setNewly(json.getString("newly"));
		}
		if (json.has("failover")) {
			job.setFailover(json.getString("failover"));
		}
		job.setFailed(json.getBoolean("isFailed"));
		job.setCurator(curator);
		job.setMem(json.getDouble("mem"));
		job.setCommand(json.getString("command"));
		return job;
	}

	private void saveState() {
		JSONObject obj = new JSONObject();
		obj.put("id", this.id);
		obj.put("command", this.command);
		obj.put("cpus", this.cpus);
		obj.put("mem", this.mem);
		if (this.newly != null) {
			obj.put("newly", this.newly);
		}
		if (this.failover != null) {
			obj.put("failover", this.failover);
		}
		obj.put("isFailed", this.isFailed);
		obj.put("status",
				(this.status == JobState.STAGING ? JobState.RUNNING : status)
						.toString());
		try {
			byte[] data = obj.toString().getBytes("UTF-8");
			try {
				curator.setData().forPath(
						ZookeeperContanst.FW_JOB_PATH + "/" + this.id, data);
			} catch (KeeperException.NoNodeException e) {
				curator.create().creatingParentsIfNeeded().forPath(
						ZookeeperContanst.FW_JOB_PATH + "/" + this.id, data);
			}
		} catch (Exception e2) {
			e2.printStackTrace();
		}
	}

	public double getCpus() {
		return cpus;
	}

	public void setCpus(double cpus) {
		this.cpus = cpus;
	}

	public double getMem() {
		return mem;
	}

	public void setMem(double mem) {
		this.mem = mem;
	}

	public String getCommand() {
		return command;
	}

	public void setCommand(String command) {
		this.command = command;
	}

	public boolean isSubmitted() {
		return submitted;
	}

	public void setSubmitted(boolean submitted) {
		this.submitted = submitted;
	}

	public JobState getStatus() {
		return status;
	}

	public void setStatus(JobState status) {
		this.status = status;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getRetries() {
		return retries;
	}

	public void setRetries(int retries) {
		this.retries = retries;
	}

	public CuratorFramework getCurator() {
		return curator;
	}

	public void setCurator(CuratorFramework curator) {
		this.curator = curator;
	}

	public SlaveID getSlaveId() {
		return slaveId;
	}

	public void setSlaveId(SlaveID slaveId) {
		this.slaveId = slaveId;
	}

	public String getNewly() {
		return newly;
	}

	public void setNewly(String newly) {
		this.newly = newly;
	}

	public String getFailover() {
		return failover;
	}

	public void setFailover(String failover) {
		this.failover = failover;
	}

	public boolean isFailed() {
		return isFailed;
	}

	public void setFailed(boolean isFailed) {
		this.isFailed = isFailed;
	}

	public String toString() {
		StringBuilder result = new StringBuilder("\n__________________\njob_");
		result.append(this.id);
		result.append("\n retry=" + this.retries);
		result.append("\n " + this.command);
		result.append("\n submitted=");
		result.append(this.submitted + "\n____________________");
		return result.toString();
	}

}
