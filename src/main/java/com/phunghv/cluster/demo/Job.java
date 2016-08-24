package com.phunghv.cluster.demo;

import java.io.UnsupportedEncodingException;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
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

	private Job() {
		this.submitted = false;
		this.status = JobState.PENDING;
		this.id = UUID.randomUUID().toString();
		this.retries = 3;
	}

	private Job(CuratorFramework curator) {
		this.submitted = false;
		this.curator = curator;
		this.status = JobState.PENDING;
		this.id = UUID.randomUUID().toString();
		this.retries = 3;
	}

	public void launch() {
		this.status = JobState.STAGING;
		saveState();
	}

	public void started() {
		this.status = JobState.RUNNING;
		saveState();
	}

	public void succeed() {
		this.status = JobState.SUCCESSFUL;
		saveState();
	}

	public void fail() {
		System.out.println("Retry count " + (4 - this.retries));
		if (this.retries == 0) {
			this.status = JobState.FAILED;
		} else {
			this.retries--;
			this.status = JobState.PENDING;
		}
		saveState();
	}

	public TaskInfo makeTask(SlaveID targetSlave) {
		UUID uuid = UUID.randomUUID();
		TaskID id = TaskID.newBuilder().setValue(uuid.toString()).build();
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
		try {
			JSONObject obj = new JSONObject();
			obj.put("id", this.id);
			obj.put("status", (this.status == JobState.STAGING
					? JobState.RUNNING : status).toString());
			byte[] data = null;
			try {
				data = obj.toString().getBytes("UTF-8");
				curator.setData().forPath("/sampleframework/jobs/" + id, data);
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch (KeeperException.NoNodeException e) {
				curator.create().creatingParentsIfNeeded()
						.forPath("/sampleframework/jobs" + id, data);
				e.printStackTrace();
			}
		} catch (Exception e) {
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

}
