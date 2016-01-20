package com.aliyun.odps.mapred.local;

/**
 * Created by petron on 16-1-5.
 */

import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Partitioner;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.TaskId;
import com.aliyun.odps.mapred.Mapper.TaskContext;
import com.aliyun.odps.mapred.bridge.ErrorCode;
import com.aliyun.odps.mapred.bridge.WritableRecord;
import com.aliyun.odps.mapred.bridge.type.ColumnBasedRecordComparator;
import com.aliyun.odps.mapred.local.CSVRecordReader;
import com.aliyun.odps.mapred.local.DriverBase;
import com.aliyun.odps.mapred.local.FileSplit;
import com.aliyun.odps.mapred.local.JobCounter;
import com.aliyun.odps.mapred.local.LocalGroupingRecordIterator;
import com.aliyun.odps.mapred.local.LocalTaskContext;
import com.aliyun.odps.mapred.local.MapOutputBuffer;
import com.aliyun.odps.mapred.local.conf.LocalConf;
import com.aliyun.odps.utils.ReflectionUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.commons.lang.ArrayUtils;
public class MapDriver extends DriverBase {
	private TaskContext mapContext;
	MapOutputBuffer outputBuffer;
	private Counters counters;

	public MapDriver(LocalConf job, FileSplit split, TaskId id, MapOutputBuffer buffer, Counters counters, TableInfo tableInfo) throws IOException {
		super(job, id, counters);
		this.outputBuffer = buffer;
		Counter mapInputByteCounter = counters.findCounter(JobCounter.MAP_INPUT_BYTES);
		Counter mapInputRecordCounter = counters.findCounter(JobCounter.MAP_INPUT_RECORDS);
		CSVRecordReader reader = new CSVRecordReader(split, mapInputRecordCounter, mapInputByteCounter, counters, job.getInputColumnSeperator());
		this.mapContext = new MapDriver.MapContextImpl(job, id, counters, reader, tableInfo);

		this.counters = counters;
	}

	public void run() throws IOException {
		Mapper mapper = ((LocalTaskContext)this.mapContext).createMapper();
		mapper.setup(this.mapContext);

		while(this.mapContext.nextRecord()) {
			mapper.map(this.mapContext.getCurrentRecordNum(), this.mapContext.getCurrentRecord(), this.mapContext);
		}

		mapper.cleanup(this.mapContext);
		((MapDriver.MapContextImpl)this.mapContext).close();
	}

	class MapContextImpl extends LocalTaskContext implements TaskContext {
		private LinkedList<Object[]> queue = new LinkedList();
		int rowNumber = 1;
		protected RecordReader reader;
		Record record;
		protected Counter mapOutputRecordCounter;
		protected TableInfo inputTableInfo;
		protected Partitioner partitioner;

		public MapContextImpl(LocalConf conf, TaskId taskid, Counters counters, RecordReader reader, TableInfo inputTableInfo) throws IOException {
			super(conf, taskid, counters);
			this.reader = reader;
			this.mapOutputRecordCounter = counters.findCounter(JobCounter.MAP_OUTPUT_RECORDS);
			this.inputTableInfo = inputTableInfo;
			Class partitionerClass;
			if(this.pipeMode) {
				String taskId = this.getTaskID().toString();
				System.err.println("Task ID: " + taskId);
				this.pipeIndex = Integer.parseInt(taskId.split("_")[0].substring(1)) - 1;
				this.pipeNode = this.pipeline.getNode(this.pipeIndex);
				conf.setMapperClass(this.pipeNode.getTransformClass());
				partitionerClass = this.pipeNode.getPartitionerClass();
			} else {
				partitionerClass = this.getJobConf().getPartitionerClass();
			}

			if(partitionerClass != null) {
				this.partitioner = (Partitioner)ReflectionUtils.newInstance(partitionerClass, this.getJobConf());
				this.partitioner.configure(conf);
			}

		}

		public long getCurrentRecordNum() {
			return (long)this.rowNumber;
		}

		public Record getCurrentRecord() {
			return this.record;
		}

		public boolean nextRecord() {
			try {
				this.record = this.reader.read();
			} catch (IOException var2) {
				throw new RuntimeException(var2);
			}

			return this.record != null;
		}

		public void write(Record record) throws IOException {
			if(this.conf.getNumReduceTasks() > 0) {
				throw new UnsupportedOperationException(ErrorCode.UNEXPECTED_MAP_WRITE_OUTPUT.toString());
			} else {
				this.mapOutputRecordCounter.increment(1L);
				this.write(record, "__default__");
			}
		}

		public void write(Record record, String label) throws IOException {
			if(this.conf.getNumReduceTasks() > 0) {
				throw new UnsupportedOperationException(ErrorCode.UNEXPECTED_MAP_WRITE_OUTPUT.toString());
			} else {
				((RecordWriter)this.recordWriters.get(label)).write(record);
				MapDriver.this.counters.findCounter(JobCounter.__EMPTY_OUTPUT_RECORD_COUNT).increment(1L);
			}
		}
		
		public void write(Record key, Record value) {
			this.mapOutputRecordCounter.increment(1L);
			this.queue.add(ArrayUtils.addAll(((WritableRecord)key).toWritableArray(), ((WritableRecord)value).toWritableArray()));
			MapDriver.this.counters.findCounter(JobCounter.__EMPTY_OUTPUT_RECORD_COUNT).increment(1L);
		}

		public TableInfo getInputTableInfo() {
			return this.inputTableInfo;
		}

		public Record createOutputKeyRecord() throws IOException {
			return null;
		}

		public Record createOutputValueRecord() throws IOException {
			return null;
		}
	
		public void close() throws IOException {
			Collections.sort(this.queue, MapDriver.this.outputBuffer.getComparator());
			Reducer combiner = (Reducer)ReflectionUtils.newInstance(this.getCombinerClass(), this.conf);
			MapDriver.MapContextImpl.CombinerContextImpl combineCtx = new MapDriver.MapContextImpl.CombinerContextImpl(this.conf, MapDriver.this.taskId, MapDriver.this.counters);
			//LOG.info("Start to run Combiner, TaskId: " + MapDriver.this.taskId);
			combiner.setup(combineCtx);

			while(combineCtx.nextKeyValue()) {
				combiner.reduce(combineCtx.getCurrentKey(), combineCtx.getValues(), combineCtx);
			}

			combiner.cleanup(combineCtx);
			this.reader.close();
			this.closeWriters();
			//LOG.info("Fininshed run Combiner, TaskId: " + MapDriver.this.taskId);
		}

		class CombinerContextImpl extends LocalTaskContext implements com.aliyun.odps.mapred.Reducer.TaskContext {
			private Record key;
			private Iterator<Record> itr;
			private Counter combineInputGroupCounter;
			private Counter combineOutputRecordCounter;

			public CombinerContextImpl(LocalConf conf, TaskId taskid, Counters counters) throws IOException {
				super(conf, taskid, counters);
				this.combineInputGroupCounter = counters.findCounter(JobCounter.COMBINE_INPUT_GROUPS);
				this.combineOutputRecordCounter = counters.findCounter(JobCounter.COMBINE_OUTPUT_RECORDS);
			}

			public boolean nextKeyValue() {
				if(this.itr == null) {
					Object[] init = (Object[])MapContextImpl.this.queue.peek();
					if(init == null) {
						return false;
					}

					this.key = this.createMapOutputKeyRecord();
					Record value = this.createMapOutputValueRecord();
					String[] groupingColumns = this.getGroupingColumns();
					ColumnBasedRecordComparator grpComparator = new ColumnBasedRecordComparator(groupingColumns, this.key.getColumns());
					this.itr = new LocalGroupingRecordIterator(MapContextImpl.this.queue, (WritableRecord)this.key, (WritableRecord)value, grpComparator, false, MapDriver.this.counters);
					this.key.set(Arrays.copyOf(init, this.key.getColumnCount()));
				} else {
					while(true) {
						if(!this.itr.hasNext()) {
							if(!((LocalGroupingRecordIterator)this.itr).reset()) {
								return false;
							}
							break;
						}

						this.itr.remove();
					}
				}

				this.combineInputGroupCounter.increment(1L);
				return true;
			}

			public Record getCurrentKey() {
				return this.key;
			}

			public Iterator<Record> getValues() {
				return this.itr;
			}

			public void write(Record record) throws IOException {
				this.write(record, "__default__");
				this.combineOutputRecordCounter.increment(1L);
			}

			public void write(Record record, String label) throws IOException {
				((RecordWriter)this.recordWriters.get(label)).write(record);
			}

			public void write(Record key, Record value) {
				if(MapContextImpl.this.partitioner != null) {
					int part = MapContextImpl.this.partitioner.getPartition(key, value, this.conf.getNumReduceTasks());
					if(part < 0 || part >= this.conf.getNumReduceTasks()) {
						throw new RuntimeException("partitioner return invalid partition value:" + part);
					}

					MapDriver.this.outputBuffer.add(key, value, part);
				} else {
					MapDriver.this.outputBuffer.add(key, value);
				}

				this.combineOutputRecordCounter.increment(1L);
			}

			public Record createOutputKeyRecord() throws IOException {
				return null;
			}

			public Record createOutputValueRecord() throws IOException {
				return null;
			}
		}
	}
}
