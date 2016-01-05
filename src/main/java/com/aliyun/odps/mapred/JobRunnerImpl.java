package com.aliyun.odps.mapred;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.conf.Configured;
import com.aliyun.odps.utils.ReflectionUtils;

public class JobRunnerImpl extends Configured implements JobRunner{
	@Override
	public RunningJob submit() throws OdpsException{
		String runner = "com.aliyun.odps.mapred.LocalJobRunner";
                JobRunner jobrunner = null;

                try {
                        Class rj = Class.forName(runner);
                        jobrunner = (JobRunner)ReflectionUtils.newInstance(rj, getConf());
                } catch (ClassNotFoundException var6) {
                        throw new RuntimeException(var6);
                }

                RunningJob rj1 = jobrunner.submit();
                System.out.println("InstanceId: " + rj1.getInstanceID());
                return rj1;
	}
}
