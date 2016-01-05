package com.aliyun.odps.mapred.local;

/**
 * Created by petron on 16-1-5.
 */

import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.mapred.TaskId;
import com.aliyun.odps.mapred.local.conf.LocalConf;
import java.io.IOException;

public abstract class DriverBase {
	protected LocalConf job;
	protected TaskId taskId;
	protected Counters counters;

	public abstract void run() throws IOException;

	public DriverBase(LocalConf job, TaskId taskId, Counters counters) throws IOException {
		this.job = job;
		this.taskId = taskId;
		this.counters = counters;
	}

	public Counters getCounters() {
		return this.counters;
	}
}
