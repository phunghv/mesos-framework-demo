package com.phunghv.cluster.demo;

import java.util.*;

import org.apache.curator.framework.CuratorFramework;
import org.apache.http.impl.io.SocketOutputBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

public class UselessRemoteBASH implements Scheduler {
	final static Logger logger = LogManager.getLogger(UselessRemoteBASH.class);
	// private boolean submitted = false;
	private List<Job> jobs = null;
	private CuratorFramework curator;

	public UselessRemoteBASH(CuratorFramework curator, List<Job> jobs) {
		this.curator = curator;
		this.jobs = jobs;
	}

	public void disconnected(SchedulerDriver schedulerDriver) {
		logger.info("Disconnect with {}", schedulerDriver.toString());

	}

	public void error(SchedulerDriver schedulerDriver, String error) {
		logger.info("framework error {}:  {}", schedulerDriver.toString(),
				error);

	}

	public void executorLost(SchedulerDriver schedulerDriver,
			ExecutorID executorID, SlaveID slaveID, int id) {

	}

	public void frameworkMessage(SchedulerDriver schedulerDriver,
			ExecutorID executorID, SlaveID slaveID, byte[] id) {
		logger.info("framework message {} {} {} ", schedulerDriver.toString(),
				executorID.toString(), slaveID.toBuilder());

	}

	public void offerRescinded(SchedulerDriver schedulerDriver,
			OfferID offerID) {

	}

	public void registered(SchedulerDriver schedulerDriver,
			FrameworkID frameworkId, MasterInfo masterInfo) {
		logger.info("Registered with framework id  {}", frameworkId.toString());
		try {
			curator.create().creatingParentsIfNeeded()
					.forPath("/sampleframework/id", frameworkId.toByteArray());
		} catch (Exception e) {
			/* Do nothing */
		}
	}

	public void reregistered(SchedulerDriver schedulerDriver,
			MasterInfo masterInfo) {
		logger.info("Re-registered framework with {} {}",
				schedulerDriver.toString(), masterInfo.toString());

		List<TaskStatus> runningTasks = new ArrayList<>();
		for (Job job : jobs) {
			System.out.println("check jobs after re register :" + job);
			if (job.getStatus() == JobState.RUNNING) {
				TaskID id = TaskID.newBuilder().setValue(job.getId()).build();
				SlaveID slaveId = SlaveID.newBuilder()
						.setValue(job.getSlaveId().toString()).build();
				System.out.println("Reconciling the task " + job.getId());
				runningTasks.add(TaskStatus.newBuilder().setSlaveId(slaveId)
						.setTaskId(id).setState(TaskState.TASK_RUNNING)
						.build());
			}
		}
		schedulerDriver.reconcileTasks(runningTasks);
	}

	public void resourceOffers(SchedulerDriver schedulerDriver,
			List<Offer> offers) {
		synchronized (jobs) {
			List<Job> pendingJobs = new ArrayList<>();
			for (Job j : jobs) {
				if (!j.isSubmitted()) {
					pendingJobs.add(j);
				}
			}
			for (Offer offer : offers) {
				if (pendingJobs.isEmpty()) {
					schedulerDriver.declineOffer(offer.getId());
					break;
				}
				Job j = pendingJobs.remove(0);
				System.out.println("Run task on " + offer.getHostname());
				TaskInfo taskInfo = j.makeTask(offer.getSlaveId());
				schedulerDriver.launchTasks(
						Collections.singletonList(offer.getId()),
						doFirstFit(offer, pendingJobs));
				j.setSubmitted(true);
				System.out.println("Lauched offer: " + taskInfo.getName());
			}
		}
	}

	public List<TaskInfo> doFirstFit(Offer offer, List<Job> jobs) {
		List<TaskInfo> toLaunch = new ArrayList<>();
		List<Job> launchedJobs = new ArrayList<>();
		double offerCpus = 0;
		double offerMem = 0;
		for (Resource resource : offer.getResourcesList()) {
			if (resource.getName().equals("cpus")) {
				offerCpus += resource.getScalar().getValue();
			} else if (resource.getName().equals("mem")) {
				offerMem += resource.getScalar().getValue();
			} else {

			}
		}
		for (Job job : jobs) {
			double jobCpus = job.getCpus();
			double jobMem = job.getMem();
			if (jobCpus <= offerCpus && jobMem <= offerMem) {
				offerCpus -= jobCpus;
				offerMem -= jobMem;
				toLaunch.add(job.makeTask(offer.getSlaveId()));
				job.setSubmitted(true);
				launchedJobs.add(job);
			}
		}
		for (Job job : launchedJobs) {
			job.launch();
		}
		jobs.removeAll(launchedJobs);
		return toLaunch;
	}

	public void slaveLost(SchedulerDriver schedulerDriver, SlaveID slaveID) {
		logger.info("lostSlave");
	}

	public void statusUpdate(SchedulerDriver schedulerDriver,
			TaskStatus status) {

		System.out.println("PhunghV got status update :" + status.getMessage());
		System.out.println("Size job = " + this.jobs.size());
		synchronized (this.jobs) {
			for (Job job : jobs) {
				System.out.println("check job " + job);
				logger.info("check job id = {} with status of task = {}",
						job.getId(), status.getState().toString());
				if (job.getId().equals(status.getTaskId().getValue())) {
					switch (status.getState()) {
					case TASK_RUNNING:
						job.started();
						break;
					case TASK_FINISHED:
						job.succeed();
						break;

					case TASK_FAILED:
					case TASK_KILLED:
					case TASK_LOST:

						job.fail();
						break;
					default:
						logger.info("check defaulllll");
						break;
					}
				}
			}
		}
	}

}
