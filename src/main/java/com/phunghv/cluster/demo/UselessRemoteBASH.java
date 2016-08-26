package com.phunghv.cluster.demo;

import java.util.*;

import org.apache.curator.framework.CuratorFramework;
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
			curator.create().creatingParentsIfNeeded().forPath(
					ZookeeperContanst.FW_ID_PATH, frameworkId.toByteArray());
		} catch (Exception e) {
			logger.error("cannot crate zookeeper id {}", e);
		}
	}

	public void reregistered(SchedulerDriver schedulerDriver,
			MasterInfo masterInfo) {
		logger.info("Re-registered framework with {} {}",
				schedulerDriver.toString(), masterInfo.toString());
	}

	public void resourceOffers(SchedulerDriver schedulerDriver,
			List<Offer> offers) {
		synchronized (jobs) {
			System.out.println("check pending job");
			List<Job> pendingJobs = new ArrayList<>();
			for (Job j : jobs) {
				System.out.println(
						"check job " + j.getId() + " : " + j.isSubmitted());
				if (!j.isSubmitted()) {
					pendingJobs.add(j);
				}
			}
			System.out.println("pending jobs size :" + pendingJobs.size());
			// change run task
			for (Offer offer : offers) {
				if (pendingJobs.isEmpty()) {
					schedulerDriver.declineOffer(offer.getId());
					break;
				}
				// schedulerDriver.launchTasks(
				// Collections.singletonList(offer.getId()),
				// doFirstFit(offer, pendingJobs));
				schedulerDriver.launchTasks(
						Collections.singletonList(offer.getId()),
						selectTask(offer, pendingJobs));
			}
		}

	}

	public List<TaskInfo> selectTask(Offer offer, List<Job> jobs) {
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
		logger.info("Total offer cpus = {} mem={}", offerCpus, offerMem);
		logger.info("size job {}", jobs.size());
		for (Job job : jobs) {
			String checkHost = (job.isFailed() ? job.getFailover()
					: job.getNewly());
			logger.info("newly = {} failover {}", job.getNewly(),
					job.getFailover());
			logger.info("checkHost = {} and {}", checkHost,
					offer.getHostname());
			if (checkHost == null
					|| checkHost.equalsIgnoreCase(offer.getHostname())) {
				logger.info("checkHost = {} matched {}", checkHost,
						offer.getHostname());
				double jobCpus = job.getCpus();
				double jobMem = job.getMem();
				logger.info("job cpus ={} mem= {}", jobCpus, jobMem);
				if (jobCpus <= offerCpus && jobMem <= offerMem) {
					offerCpus -= jobCpus;
					offerMem -= jobMem;
					toLaunch.add(job.makeTask(offer.getSlaveId()));
					job.setSubmitted(true);
					launchedJobs.add(job);
				}
			}
		}
		logger.info("Size launchedJob : {}", launchedJobs.size());
		logger.info("Size toLaunch : {}", toLaunch.size());
		for (Job job : launchedJobs) {
			job.launch();
		}
		jobs.removeAll(launchedJobs);
		return toLaunch;
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

		logger.info("Total offer cpus = {} mem={}", offerCpus, offerMem);
		logger.info("size job {}", jobs.size());
		for (Job job : jobs) {
			double jobCpus = job.getCpus();
			double jobMem = job.getMem();
			logger.info("job cpus ={} mem= {}", jobCpus, jobMem);
			if (jobCpus <= offerCpus && jobMem <= offerMem) {
				offerCpus -= jobCpus;
				offerMem -= jobMem;
				toLaunch.add(job.makeTask(offer.getSlaveId()));
				job.setSubmitted(true);
				launchedJobs.add(job);
			}
		}
		logger.info("Size launchedJob : {}", launchedJobs.size());
		logger.info("Size toLaunch : {}", toLaunch.size());
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
						break;
					}
				}
			}
		}
	}

}
