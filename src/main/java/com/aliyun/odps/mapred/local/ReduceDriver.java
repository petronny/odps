package com.aliyun.odps.mapred.local;

/**
 * Created by petron on 16-1-5.
 */

import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.mapred.Partitioner;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.TaskId;
import com.aliyun.odps.mapred.Reducer.TaskContext;
import com.aliyun.odps.mapred.bridge.ErrorCode;
import com.aliyun.odps.mapred.bridge.WritableRecord;
import com.aliyun.odps.mapred.bridge.type.ColumnBasedRecordComparator;
import com.aliyun.odps.mapred.local.DriverBase;
import com.aliyun.odps.mapred.local.JobCounter;
import com.aliyun.odps.mapred.local.LocalGroupingRecordIterator;
import com.aliyun.odps.mapred.local.LocalTaskContext;
import com.aliyun.odps.mapred.local.MapOutputBuffer;
import com.aliyun.odps.mapred.local.conf.LocalConf;
import com.aliyun.odps.utils.ReflectionUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Queue;


public class ReduceDriver extends DriverBase{
	private final TaskContext reduceContext;
	MapOutputBuffer inputBuffer;
	MapOutputBuffer outputBuffer;

	public ReduceDriver(LocalConf job,MapOutputBuffer inputBuffer,MapOutputBuffer outputBuffer,TaskId id,Counters counters,int partitionIndex) throws IOException{
		super(job,id,counters);
		this.inputBuffer=inputBuffer;
		this.outputBuffer=outputBuffer;
		this.reduceContext=new ReduceDriver.ReduceContextImpl(job,this.taskId,counters,inputBuffer.getPartitionQueue(partitionIndex),outputBuffer);
	}

	public void run() throws IOException{
		Reducer reducer=((LocalTaskContext)this.reduceContext).createReducer();
		reducer.setup(this.reduceContext);

		while(this.reduceContext.nextKeyValue()){
			reducer.reduce(this.reduceContext.getCurrentKey(),this.reduceContext.getValues(),this.reduceContext);
		}

		reducer.cleanup(this.reduceContext);
		((ReduceDriver.ReduceContextImpl)this.reduceContext).close();
	}

	class ReduceContextImpl extends LocalTaskContext implements TaskContext{
		private Record key;
		private Comparator<Object[]> keyGroupingComparator;
		private LocalGroupingRecordIterator itr;
		private Queue<Object[]> queue;
		private MapOutputBuffer outputBuffer;
		private Partitioner partitioner;
		private Counters counters;

		public ReduceContextImpl(LocalConf job,TaskId taskId,Counters counters,Queue<Object[]> queue,MapOutputBuffer var1) throws IOException{
			super(job,taskId,counters);
			Class partitionerClass=null;
			if(this.pipeMode){
				String taskIdStr=this.getTaskID().toString();
				System.err.println("Task ID: "+taskIdStr);
				this.pipeIndex=Integer.parseInt(taskIdStr.split("_")[0].substring(1))-1;
				this.pipeNode=this.pipeline.getNode(this.pipeIndex);
				this.conf.setReducerClass(this.pipeNode.getTransformClass());
				this.key=new WritableRecord(this.pipeNode.getInputKeySchema());
				this.keyGroupingComparator=new ColumnBasedRecordComparator(this.pipeNode.getInputGroupingColumns(),this.key.getColumns());
				partitionerClass=this.pipeNode.getPartitionerClass();
			}else{
				this.key=new WritableRecord(this.conf.getMapOutputKeySchema());
				this.keyGroupingComparator=new ColumnBasedRecordComparator(this.conf.getOutputGroupingColumns(),this.key.getColumns());
			}

			if(partitionerClass!=null){
				this.partitioner=(Partitioner)ReflectionUtils.newInstance(partitionerClass,this.getJobConf());
				this.partitioner.configure(this.conf);
			}

			this.queue=queue;
			this.outputBuffer=outputBuffer;
			this.counters=counters;
		}

		public void write(Record record) throws IOException{
			this.write(record,"__default__");
		}

		public void write(Record record,String label) throws IOException{
			((RecordWriter)this.recordWriters.get(label)).write(record);
			this.counters.findCounter(JobCounter.__EMPTY_OUTPUT_RECORD_COUNT).increment(1L);
		}

		public boolean nextKeyValue(){
			if(this.itr==null){
				Object[] init=(Object[])this.queue.peek();
				if(init==null){
					return false;
				}

				WritableRecord value;
				if(this.pipeMode){
					value=new WritableRecord(this.pipeNode.getInputValueSchema());
				}else{
					value=new WritableRecord(this.conf.getMapOutputValueSchema());
				}

				this.itr=new LocalGroupingRecordIterator(this.queue,(WritableRecord)this.key,(WritableRecord)value,this.keyGroupingComparator,true,this.counters);
				this.key.set(Arrays.copyOf(init,this.key.getColumnCount()));
			}else{
				while(true){
					if(!this.itr.hasNext()){
						if(!this.itr.reset()){
							return false;
						}
						break;
					}

					this.itr.remove();
				}
			}

			return true;
		}

		public Record getCurrentKey(){
			return this.key;
		}

		public Iterator<Record> getValues(){
			return this.itr;
		}

		public void write(Record key,Record value){
			if(this.pipeMode&&this.pipeNode!=null){
				if(this.partitioner!=null){
					int part=this.partitioner.getPartition(key,value,this.conf.getNumReduceTasks());
					if(part<0||part>=this.conf.getNumReduceTasks()){
						throw new RuntimeException("partitioner return invalid partition value:"+part);
					}

					this.outputBuffer.add(key,value,part);
				}else{
					this.outputBuffer.add(key,value);
				}

				this.counters.findCounter(JobCounter.__EMPTY_OUTPUT_RECORD_COUNT).increment(1L);
			}else{
				throw new UnsupportedOperationException(ErrorCode.INTERMEDIATE_OUTPUT_IN_REDUCER.toString());
			}
		}

		public void close() throws IOException{
			this.closeWriters();
		}
	}
}