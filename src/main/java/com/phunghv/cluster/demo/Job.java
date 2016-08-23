package com.phunghv.cluster.demo;

import java.util.UUID;

import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Value;

import org.json.*;

public class Job {
	private double cpus;
	private double mem;
	private String command;
	private boolean submitted;
	private JobState status;
	private String id;
	private int retries;

	private Job() {
		this.submitted = false;
		this.status = JobState.PENDING;
		this.id = UUID.randomUUID().toString();
		this.retries = 3;
	}

	public void launch() {
		this.status = JobState.STAGING;
	}

	public void started() {
		this.status = JobState.RUNNING;
	}

	public void succeed() {
		this.status = JobState.SUCCESSFUL;
	}

	public void fail() {
		System.out.println("Retry count " + (4 - this.retries));
		if (this.retries == 0) {
			this.status = JobState.FAILED;
		} else {
			this.retries--;
			this.status = JobState.PENDING;
		}
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

}
