package com.phunghv.cluster.demo;

import java.util.Collection;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.Filters;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Request;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.SchedulerDriver;

public class CustomSchedulerDriver implements SchedulerDriver {

	@Override
	public Status abort() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status declineOffer(OfferID arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status declineOffer(OfferID arg0, Filters arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status join() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status killTask(TaskID arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status launchTasks(Collection<OfferID> arg0,
			Collection<TaskInfo> arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status launchTasks(OfferID arg0, Collection<TaskInfo> arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status launchTasks(Collection<OfferID> arg0,
			Collection<TaskInfo> arg1, Filters arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status launchTasks(OfferID arg0, Collection<TaskInfo> arg1,
			Filters arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status reconcileTasks(Collection<TaskStatus> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status requestResources(Collection<Request> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status reviveOffers() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status run() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status sendFrameworkMessage(ExecutorID arg0, SlaveID arg1,
			byte[] arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status start() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status stop() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status stop(boolean arg0) {
		// TODO Auto-generated method stub
		return null;
	}

}
