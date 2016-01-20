package com.aliyun.odps.mapred;

/**
 * Created by petron on 16-1-5.
 */
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobContext;
import com.aliyun.odps.mapred.TaskId;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Iterator;

public interface TaskContext extends JobContext {
	TaskId getTaskID();

	TableInfo[] getOutputTableInfo() throws IOException;

	Record createOutputRecord() throws IOException;

	Record createOutputRecord(String var1) throws IOException;

	Record createOutputKeyRecord() throws IOException;

	Record createOutputValueRecord() throws IOException;

	Record createMapOutputKeyRecord() throws IOException;

	Record createMapOutputValueRecord() throws IOException;

	BufferedInputStream readResourceFileAsStream(String var1) throws IOException;

	Iterator<Record> readResourceTable(String var1) throws IOException;

	Counter getCounter(Enum<?> var1);

	Counter getCounter(String var1, String var2);

	void progress();

	void write(Record var1) throws IOException;

	void write(Record var1, String var2) throws IOException;

	void write(Record var1, Record var2) throws IOException;
}

