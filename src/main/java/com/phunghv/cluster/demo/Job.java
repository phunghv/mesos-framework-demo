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

	final static Logger logger = LogManager.getLogger(Job.class);

	private Job() {
		this.submitted = false;
		this.status = JobState.PENDING;
		this.id = UUID.randomUUID().toString();
		this.retries = 10;
	}

	private Job(CuratorFramework curator) {
		this.submitted = false;
		this.curator = curator;
		this.status = JobState.PENDING;
		this.id = UUID.randomUUID().toString();
		this.retries = 10;
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
		logger.info("retry job");
		System.out.println("Retry count " + (4 - this.retries));
		if (this.retries == 0) {
			logger.info("job failed");
			this.status = JobState.FAILED;
		} else {
			this.retries--;
			logger.info("retry job count {}", this.retries);
			this.status = JobState.PENDING;
		}
		saveState();
	}

	public TaskInfo makeTask(SlaveID targetSlave) {
		logger.info("make task on {}", targetSlave.toString());
//		UUID uuid = UUID.randomUUID();
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
		return job;
	}

	public static Job fromJSON(JSONObject json, CuratorFramework curator)
			throws JSONException {
		Job job = new Job();
		job.setCpus(json.getDouble("cpus"));
		job.setCurator(curator);
		job.setMem(json.getDouble("mem"));
		job.setCommand(json.getString("command"));
		return job;
	}

	private void saveState() {
		logger.info("save state job id={}", this.id);
		try {
			JSONObject obj = new JSONObject();
			obj.put("id", this.id);
			obj.put("cmd", this.command);
			obj.put("cpus", this.cpus);
			obj.put("mem", this.mem);
			obj.put("status", (this.status == JobState.STAGING
					? JobState.RUNNING : status).toString());
			byte[] data = obj.toString().getBytes("UTF-8");
			try {
				curator.setData().forPath("/sampleframework/jobs/" + id, data);
			} catch (KeeperException.NoNodeException e) {
				curator.create().creatingParentsIfNeeded()
						.forPath("/sampleframework/jobs/" + id, data);
			}
			logger.info("Save state of job : " + obj.toString(3));
		} catch (Exception e) {
			System.out.println("Cannot create parrent node");
			e.printStackTrace();
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
