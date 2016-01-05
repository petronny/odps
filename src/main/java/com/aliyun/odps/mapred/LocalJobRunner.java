package com.aliyun.odps.mapred;
/**
 * Created by petron on 16-1-5.
 */

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.CounterGroup;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobRunner;
import com.aliyun.odps.mapred.JobStatus;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.TaskId;
import com.aliyun.odps.mapred.bridge.utils.Validator;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.mapred.local.FileSplit;
import com.aliyun.odps.mapred.local.JobCounter;
import com.aliyun.odps.mapred.local.LocalRunningJob;
import com.aliyun.odps.mapred.local.LocalTaskId;
import com.aliyun.odps.mapred.local.MapDriver;
import com.aliyun.odps.mapred.local.MapOutputBuffer;
import com.aliyun.odps.mapred.local.ReduceDriver;
import com.aliyun.odps.mapred.local.StageStatic;
import com.aliyun.odps.mapred.local.TableMeta;
import com.aliyun.odps.mapred.local.base.JobDirecotry;
import com.aliyun.odps.mapred.local.base.WareHouse;
import com.aliyun.odps.mapred.local.conf.LocalConf;
import com.aliyun.odps.mapred.local.utils.CommonUtils;
import com.aliyun.odps.mapred.local.utils.DownloadUtils;
import com.aliyun.odps.mapred.local.utils.LocalRunUtils;
import com.aliyun.odps.mapred.local.utils.LocalValidatorFactory;
import com.aliyun.odps.mapred.local.utils.PartitionUtils;
import com.aliyun.odps.mapred.local.utils.SchemaUtils;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.pipeline.Pipeline;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LocalJobRunner implements JobRunner{
	private List<FileSplit> inputs;
	private WareHouse wareHouse;
	private JobDirecotry jobDirecotry;
	private Counters counters;
	private Odps odps;
	private LocalConf conf;
	private Map<FileSplit,TableInfo> splitToTableInfo;
	private List<StageStatic> stageStaticList;
	private static final Log LOG=LogFactory.getLog(LocalJobRunner.class);
	public static Counter EMPTY_COUNTER;
	private Pipeline pipeline;

	public LocalJobRunner(){
	}

	public void initialize(){
		CommonUtils.generateLocalMrTaskName(this.conf);
		this.inputs=new ArrayList();
		this.counters=new Counters();
		this.odps=SessionState.get().getOdps();
		this.splitToTableInfo=new HashMap();
		this.stageStaticList=new LinkedList();
		this.wareHouse=WareHouse.getInstance();
		this.wareHouse.setOdps(this.odps);
		this.jobDirecotry=new JobDirecotry(this.conf);
		EMPTY_COUNTER=this.counters.findCounter(JobCounter.__EMPTY_WILL_NOT_SHOW);
	}

	public RunningJob submit(){
		try{
			this.initialize();
			this.runJob();
			return new LocalRunningJob(this.conf.getJobName(),JobStatus.SUCCEEDED,this.counters);
		}catch(Exception var2){
			throw new RuntimeException(var2);
		}
	}

	private void runJob() throws IOException, OdpsException{
		this.pipeline=Pipeline.fromJobConf(this.conf);
		LOG.info("Run mapreduce job in local mode, Type: "+(this.pipeline==null?"MR":"MRR")+", Job ID: "+this.conf.getJobName());
		this.jobDirecotry.writeJobConf();
		LOG.info("Start to process input tables");
		this.processInputs();
		LOG.info("Finished process input tables");
		LOG.info("Start to process output tables");
		this.processOutputs();
		LOG.info("Finished process output tables");
		LOG.info("Start to process resources");
		this.processResources();
		LOG.info("Finished process resources");
		LOG.info("Start to fill tableInfo");
		this.fillTableInfo();
		LOG.info("Finished fill tableInfo");
		LOG.info("Start to validate configuration");
		Validator validator=LocalValidatorFactory.getValidator(this.conf);
		validator.validate();
		LOG.info("Finished validate configuration");
		if(this.pipeline!=null){
			this.handlePipeMode();
		}else{
			this.handleNonPipeMode();
		}

		this.moveOutputs();

		try{
			if(!this.conf.isRetainTempData()){
				FileUtils.deleteDirectory(this.jobDirecotry.getJobDir());
			}
		}catch(Exception var3){
			LOG.warn(var3.getMessage());
		}

		System.err.println();
		System.err.println("Summary:");
		this.printInputOutput();
		this.printStageStatic();
		this.printCounters();
		System.err.println("\nOK");
	}

	private void handlePipeMode() throws IOException{
		boolean mapCopyNum=false;
		boolean reduceCopyNum=false;
		int var11;
		if(this.inputs.size()>0){
			var11=this.inputs.size();
		}else{
			var11=this.conf.getInt("odps.stage.mapper.num",1);
		}

		int var12=this.computeReduceNum(var11);
		LOG.info("Start to run mappers, num: "+var11);
		LocalTaskId taskId=new LocalTaskId("M1",0,this.odps.getDefaultProject());
		StageStatic stageStatic=this.createStageStatic(taskId);
		stageStatic.setWorkerCount(var11);
		MapOutputBuffer inputBuffer=new MapOutputBuffer(this.conf,this.pipeline,taskId.getTaskId());

		int reduceNodeCount;
		for(reduceNodeCount=0;reduceNodeCount<var11;++reduceNodeCount){
			FileSplit i=this.inputs.size()>0?(FileSplit)this.inputs.get(reduceNodeCount):FileSplit.NullSplit;
			taskId=new LocalTaskId("M1",0,this.odps.getDefaultProject());
			LOG.info("Start to run mapper, TaskId: "+taskId+", Input: "+this.splitToTableInfo.get(i));
			MapDriver outputBuffer=new MapDriver(this.conf,i,taskId,inputBuffer,this.counters,(TableInfo)this.splitToTableInfo.get(i));
			outputBuffer.run();
			this.setInputOutputRecordCount(stageStatic);
			LOG.info("Fininshed run mapper, TaskId: "+taskId+", Input: "+this.splitToTableInfo.get(i));
		}

		LOG.info("Fininshed run all mappers, num: "+var11);
		reduceNodeCount=this.pipeline.getNodeNum()-1;
		if(reduceNodeCount>0){
			LOG.info("Start to run reduces, num: "+reduceNodeCount);
			stageStatic.setNextTaskId("R2_1");

			int var13;
			for(var13=0;var13<reduceNodeCount;++var13){
				taskId=new LocalTaskId("R"+(var13+2)+"_"+(var13+1),0,this.odps.getDefaultProject());
				LOG.info("Start to run reduce, taskId: "+taskId);
				stageStatic.setNextTaskId("R"+(var13+2)+"_"+(var13+1));
				stageStatic=this.createStageStatic(taskId);
				stageStatic.setWorkerCount(var12);
				MapOutputBuffer var14=new MapOutputBuffer(this.conf,this.pipeline,taskId.getTaskId());

				for(int j=0;j<var12;++j){
					ReduceDriver reduceDriver=new ReduceDriver(this.conf,inputBuffer,var14,taskId,this.counters,j);
					reduceDriver.run();
					this.setInputOutputRecordCount(stageStatic);
				}

				inputBuffer=var14;
				var12=this.computeReduceNum(var12);
				LOG.info("Finished run reduce, taskId: "+taskId);
			}

			stageStatic.setNextTaskId("R"+(var13+1)+"_"+var13+"FS_9");
			LOG.info("Fininshed run all reduces, num: "+reduceNodeCount);
		}else{
			stageStatic.setNextTaskId("M1");
			LOG.info("This is a MapOnly job");
		}

	}

	private void handleNonPipeMode() throws IOException{
		boolean mapCopyNum=false;
		boolean reduceCopyNum=false;
		int var9;
		if(this.inputs.size()>0){
			var9=this.inputs.size();
		}else{
			var9=this.conf.getInt("odps.stage.mapper.num",1);
		}

		int var10=this.computeReduceNum(var9);
		MapOutputBuffer buffer=new MapOutputBuffer(this.conf);
		LOG.info("Start to run mappers, num: "+var9);
		LocalTaskId taskId=new LocalTaskId("M1",0,"demo");
		StageStatic stageStatic=this.createStageStatic(taskId);
		stageStatic.setWorkerCount(var9);

		int reduceId;
		TaskId var11;
		for(reduceId=0;reduceId<var9;++reduceId){
			FileSplit reduceDriver=this.inputs.size()>0?(FileSplit)this.inputs.get(reduceId):FileSplit.NullSplit;
			var11=new TaskId("M",reduceId+1);
			LOG.info("Start to run mapper, TaskId: "+var11+", Input: "+this.splitToTableInfo.get(reduceDriver));
			MapDriver mapDriver=new MapDriver(this.conf,reduceDriver,var11,buffer,this.counters,(TableInfo)this.splitToTableInfo.get(reduceDriver));
			mapDriver.run();
			this.setInputOutputRecordCount(stageStatic);
			LOG.info("Fininshed run mapper, TaskId: "+var11+", Input: "+this.splitToTableInfo.get(reduceDriver));
		}

		LOG.info("Fininshed run all mappers, num: "+var9);
		if(var10>0){
			LOG.info("Start to run reduces, num: "+var10);
			taskId=new LocalTaskId("R2_1",0,"demo");
			stageStatic.setNextTaskId("R2_1");
			stageStatic=this.createStageStatic(taskId);
			stageStatic.setWorkerCount(var10);

			for(reduceId=0;reduceId<var10;++reduceId){
				var11=new TaskId("R",reduceId);
				LOG.info("Start to run reduce, taskId: "+var11);
				ReduceDriver var12=new ReduceDriver(this.conf,buffer,(MapOutputBuffer)null,var11,this.counters,reduceId);
				var12.run();
				this.setInputOutputRecordCount(stageStatic);
				LOG.info("Finished run reduce, taskId: "+var11);
			}

			stageStatic.setNextTaskId("R2_1FS_9");
			LOG.info("Fininshed run all reduces, num: "+var10);
		}else{
			stageStatic.setNextTaskId("M1");
			LOG.info("This is a MapOnly job");
		}

	}

	private StageStatic createStageStatic(TaskId taskId){
		StageStatic stageStatic=new StageStatic();
		this.stageStaticList.add(stageStatic);
		stageStatic.setTaskId(taskId.toString());
		return stageStatic;
	}

	private void setInputOutputRecordCount(StageStatic stageStatic){
		stageStatic.setInputRecordCount(this.counters.findCounter(JobCounter.__EMPTY_INPUT_RECORD_COUNT).getValue());
		stageStatic.setOutputRecordCount(this.counters.findCounter(JobCounter.__EMPTY_OUTPUT_RECORD_COUNT).getValue());
		this.counters.findCounter(JobCounter.__EMPTY_INPUT_RECORD_COUNT).setValue(0L);
		this.counters.findCounter(JobCounter.__EMPTY_OUTPUT_RECORD_COUNT).setValue(0L);
	}

	private void processInput(TableInfo tableInfo) throws IOException, OdpsException{
		System.out.println(tableInfo.getTableName());
		if(tableInfo!=null&&!StringUtils.isBlank(tableInfo.getTableName())){
			if(StringUtils.isEmpty(tableInfo.getProjectName())){
				tableInfo.setProjectName("demo");
			}

			String[] readCols=tableInfo.getCols();
			PartitionSpec expectParts=tableInfo.getPartitionSpec();
			if(!this.wareHouse.existsPartition(tableInfo.getProjectName(),tableInfo.getTableName(),expectParts)){
				//DownloadUtils.downloadTableSchemeAndData(this.odps,tableInfo,this.conf.getLimitDownloadRecordCount(),this.conf.getInputColumnSeperator());
				if(!this.wareHouse.existsPartition(tableInfo.getProjectName(),tableInfo.getTableName(),expectParts)){
					throw new RuntimeException(LocalRunUtils.getDownloadErrorMsg(tableInfo.toString()));
				}
			}

			TableMeta whTblMeta=this.wareHouse.getTableMeta(tableInfo.getProjectName(),tableInfo.getTableName());
			Column[] whReadFields=LocalRunUtils.getInputTableFields(whTblMeta,readCols);
			List whParts=this.wareHouse.getPartitions(tableInfo.getProjectName(),tableInfo.getTableName());
			File file;
			if(whParts.size()>0){
				Iterator whSrcDir=whParts.iterator();

				while(true){
					PartitionSpec tempDataDir;
					File tempSchemeDir;
					do{
						do{
							if(!whSrcDir.hasNext()){
								return;
							}

							tempDataDir=(PartitionSpec)whSrcDir.next();
						}while(!PartitionUtils.match(expectParts,tempDataDir));

						tempSchemeDir=this.wareHouse.getPartitionDir(whTblMeta.getProjName(),whTblMeta.getTableName(),tempDataDir);
					}while(LocalRunUtils.listDataFiles(tempSchemeDir).size()<=0);

					File i$=this.jobDirecotry.getInputDir(this.wareHouse.getRelativePath(whTblMeta.getProjName(),whTblMeta.getTableName(),tempDataDir,new Object[0]));
					file=this.jobDirecotry.getInputDir(this.wareHouse.getRelativePath(whTblMeta.getProjName(),whTblMeta.getTableName(),(PartitionSpec)null,new Object[0]));
					this.wareHouse.copyTable(whTblMeta.getProjName(),whTblMeta.getTableName(),tempDataDir,readCols,file,this.conf.getLimitDownloadRecordCount(),this.conf.getInputColumnSeperator());
					Iterator split=LocalRunUtils.listDataFiles(i$).iterator();

					while(split.hasNext()){
						File file1=(File)split.next();
						FileSplit split1=new FileSplit(file1,whReadFields,0L,file1.length());
						this.splitToTableInfo.put(split1,tableInfo);
						this.inputs.add(split1);
					}
				}
			}else{
				if(tableInfo.getPartSpec()!=null&&tableInfo.getPartSpec().size()>0){
					throw new IOException("ODPS-0720121: Invalid table partSpectable "+tableInfo.getProjectName()+"."+tableInfo.getTableName()+" is not partitioned table");
				}

				File whSrcDir1=this.wareHouse.getTableDir(whTblMeta.getProjName(),whTblMeta.getTableName());
				if(LocalRunUtils.listDataFiles(whSrcDir1).size()>0){
					File tempDataDir1=this.jobDirecotry.getInputDir(this.wareHouse.getRelativePath(whTblMeta.getProjName(),whTblMeta.getTableName(),(PartitionSpec)null,new Object[0]));
					this.wareHouse.copyTable(whTblMeta.getProjName(),whTblMeta.getTableName(),(PartitionSpec)null,readCols,tempDataDir1,this.conf.getLimitDownloadRecordCount(),this.conf.getInputColumnSeperator());
					Iterator i$1=LocalRunUtils.listDataFiles(tempDataDir1).iterator();

					while(i$1.hasNext()){
						file=(File)i$1.next();
						FileSplit split2=new FileSplit(file,whReadFields,0L,file.length());
						this.splitToTableInfo.put(split2,tableInfo);
						this.inputs.add(split2);
					}
				}
			}

		}else{
			throw new RuntimeException("Invalid TableInfo: "+tableInfo);
		}
	}

	private void processInputs() throws IOException, OdpsException{
		TableInfo[] inputTableInfos=InputUtils.getTables(this.conf);
		if(inputTableInfos==null){
			LOG.debug("No input tables to process");
		}else{
			TableInfo[] arr$=inputTableInfos;
			int len$=inputTableInfos.length;

			for(int i$=0;i$<len$;++i$){
				TableInfo tableInfo=arr$[i$];
				LOG.debug("Start to process input table: "+tableInfo);
				this.processInput(tableInfo);
				LOG.debug("Finished process input table: "+tableInfo);
			}

			if(this.inputs.isEmpty()){
				this.inputs.add(FileSplit.NullSplit);
			}

		}
	}

	private void processResources() throws IOException, OdpsException{
		String[] resources=this.conf.getResources();
		if(resources!=null&&resources.length!=0){
			HashSet names=new HashSet(Arrays.asList(resources));
			LOG.info("Start to process resources: "+StringUtils.join(resources,','));
			URLClassLoader loader=(URLClassLoader)Thread.currentThread().getContextClassLoader();
			ArrayList cp=new ArrayList(Arrays.asList(loader.getURLs()));
			String curProjName=this.wareHouse.getOdps().getDefaultProject();
			File resDir=this.jobDirecotry.getResourceDir();
			Iterator newLoader=names.iterator();

			while(newLoader.hasNext()){
				String name=(String)newLoader.next();
				List res=LocalRunUtils.parseResourceName(name,curProjName);
				String projName=(String)res.get(0);
				String resName=(String)res.get(1);
				if(!this.wareHouse.existsResource(projName,resName)){
					DownloadUtils.downloadResource(this.odps,projName,resName,this.conf.getLimitDownloadRecordCount(),this.conf.getInputColumnSeperator());
				}

				this.wareHouse.copyResource(projName,resName,resDir,this.conf.getLimitDownloadRecordCount(),this.conf.getInputColumnSeperator());
				cp.add((new File(resDir,resName)).toURL());
			}

			URLClassLoader newLoader1=new URLClassLoader((URL[])cp.toArray(new URL[0]),loader);
			Thread.currentThread().setContextClassLoader(newLoader1);
			this.conf.setClassLoader(newLoader1);
		}else{
			LOG.debug("No resources to process");
		}
	}

	private void processOutputs() throws IOException{
		TableInfo[] outputs=OutputUtils.getTables(this.conf);
		if(outputs!=null&&outputs.length!=0){
			TableInfo[] arr$=outputs;
			int len$=outputs.length;

			for(int i$=0;i$<len$;++i$){
				TableInfo tableInfo=arr$[i$];
				if(StringUtils.isBlank(tableInfo.getProjectName())){
					tableInfo.setProjectName("demo");
				}

				File tableDirInJobDir=this.jobDirecotry.getOutputDir(tableInfo);
				tableDirInJobDir.mkdirs();
				TableMeta tblMeta=null;
				if(this.wareHouse.existsTable(tableInfo.getProjectName(),tableInfo.getTableName())){
					tblMeta=this.wareHouse.getTableMeta(tableInfo.getProjectName(),tableInfo.getTableName());
				}else{
					tblMeta=DownloadUtils.downloadTableInfo(this.odps,tableInfo);
					File tableDirInWarehouse=this.wareHouse.getTableDir(tableInfo.getProjectName(),tableInfo.getTableName());
					tableDirInWarehouse.mkdirs();
					SchemaUtils.generateSchemaFile(tblMeta,(List)null,tableDirInWarehouse);
				}

				SchemaUtils.generateSchemaFile(tblMeta,(List)null,tableDirInJobDir);
				this.conf.setOutputSchema(tblMeta.getCols(),tableInfo.getLabel());
			}

		}else{
			LOG.debug("No output tables to process");
		}
	}

	private void fillTableInfo() throws IOException{
		TableInfo[] infos=new TableInfo[this.splitToTableInfo.size()];
		this.splitToTableInfo.values().toArray(infos);
		String project="demo";
		Iterator changed=this.splitToTableInfo.keySet().iterator();

		TableInfo len$;
		Column[] i$;
		Column[] info;
		int schema;
		String colName;
		Column[] arr$1;
		int len$1;
		int i$1;
		Column c;
		while(changed.hasNext()){
			FileSplit arr$=(FileSplit)changed.next();
			len$=(TableInfo)this.splitToTableInfo.get(arr$);
			if(len$.getProjectName()==null){
				len$.setProjectName(project);
			}

			i$=this.wareHouse.getTableMeta(len$.getProjectName(),len$.getTableName()).getCols();
			if(len$.getCols()==null){
				this.conf.setInputSchema(len$,i$);
				len$.setCols(SchemaUtils.getColumnNames(i$));
			}else{
				info=new Column[len$.getCols().length];

				for(schema=0;schema<len$.getCols().length;++schema){
					colName=len$.getCols()[schema];
					arr$1=i$;
					len$1=i$.length;

					for(i$1=0;i$1<len$1;++i$1){
						c=arr$1[i$1];
						if(c.getName().equalsIgnoreCase(colName)){
							info[schema]=c;
							break;
						}
					}
				}

				this.conf.setInputSchema(len$,info);
			}
		}

		infos=InputUtils.getTables(this.conf);
		boolean var14=false;

		for(int var15=0;var15<infos.length;++var15){
			len$=infos[var15];
			if(len$.getProjectName()==null){
				var14=true;
				len$.setProjectName(project);
			}

			i$=this.wareHouse.getTableMeta(len$.getProjectName(),len$.getTableName()).getCols();
			if(len$.getCols()==null){
				var14=true;
				this.conf.setInputSchema(len$,i$);
				len$.setCols(SchemaUtils.getColumnNames(i$));
			}else{
				info=new Column[len$.getCols().length];

				for(schema=0;schema<len$.getCols().length;++schema){
					colName=len$.getCols()[schema];
					arr$1=i$;
					len$1=i$.length;

					for(i$1=0;i$1<len$1;++i$1){
						c=arr$1[i$1];
						if(c.getName().equalsIgnoreCase(colName)){
							info[schema]=c;
							break;
						}
					}
				}

				this.conf.setInputSchema(len$,info);
			}

			infos[var15]=len$;
		}

		if(var14){
			InputUtils.setTables(infos,this.conf);
		}

		infos=OutputUtils.getTables(this.conf);
		if(infos==null){
			this.conf.setOutputSchema(new Column[]{new Column("nil",OdpsType.STRING)},"__default__");
		}else{
			TableInfo[] var16=infos;
			int var17=infos.length;

			for(int var18=0;var18<var17;++var18){
				TableInfo var19=var16[var18];
				if(var19.getProjectName()==null){
					var19.setProjectName(project);
				}

				Column[] var20=this.wareHouse.getTableMeta(var19.getProjectName(),var19.getTableName()).getCols();
				var19.setCols(SchemaUtils.getColumnNames(var20));
				this.conf.setOutputSchema(var20,var19.getLabel());
			}

			OutputUtils.setTables(infos,this.conf);
		}

	}

	private void moveOutputs() throws IOException{
		TableInfo[] output=OutputUtils.getTables(this.conf);
		if(output!=null){
			TableInfo[] arr$=output;
			int len$=output.length;

			for(int i$=0;i$<len$;++i$){
				TableInfo table=arr$[i$];
				String label=table.getLabel();
				String projName=table.getProjectName();
				if(projName==null){
					projName=this.wareHouse.getOdps().getDefaultProject();
				}

				String tblName=table.getTableName();
				LinkedHashMap partSpec=table.getPartSpec();
				File tempTblDir=this.jobDirecotry.getOutputDir(table);
				File whOutputDir=this.wareHouse.createPartitionDir(projName,tblName,PartitionUtils.convert(partSpec));
				if(this.wareHouse.existsTable(projName,tblName)){
					LOG.info("Reload warehouse table:"+tblName);
					LocalRunUtils.removeDataFiles(whOutputDir);
					this.wareHouse.copyDataFiles(tempTblDir,(List)null,whOutputDir,this.conf.getInputColumnSeperator());
				}else{
					LOG.info("Copy output to warehouse: label="+label+" -> "+whOutputDir.getAbsolutePath());
					File whOutputTableDir=this.wareHouse.getTableDir(projName,tblName);
					FileUtils.copyDirectory(tempTblDir,whOutputTableDir,new FileFilter(){
						public boolean accept(File pathname){
							String filename=pathname.getName();
							return filename.equals("__schema__");
						}
					});
					FileUtils.copyDirectory(tempTblDir,whOutputDir,new FileFilter(){
						public boolean accept(File pathname){
							String filename=pathname.getName();
							return !filename.equals("__schema__");
						}
					});
				}
			}

		}
	}

	private int computeReduceNum(int mapNum) throws IOException{
		boolean reduceNum=true;
		int reduceNum1;
		if(this.conf.caintainsKey("odps.stage.reducer.num")){
			reduceNum1=this.conf.getNumReduceTasks();
		}else{
			reduceNum1=Math.max(1,mapNum/4);
		}

		if(reduceNum1<0){
			throw new IOException("ODPS-0720251: Num of reduce instance is invalid - reduce num cann\'t be less than 0");
		}else{
			if(reduceNum1!=this.conf.getNumReduceTasks()){
				LOG.info("change reduce num from "+this.conf.getNumReduceTasks()+" to "+reduceNum1);
			}

			this.conf.setNumReduceTasks(reduceNum1);
			return reduceNum1;
		}
	}

	private void printInputOutput(){
		StringBuffer sb=new StringBuffer();
		System.err.println("Inputs:");
		TableInfo[] tableInfos=InputUtils.getTables(this.conf);
		TableInfo[] arr$;
		int len$;
		int i$;
		TableInfo tableInfo;
		String parts;
		if(tableInfos!=null){
			arr$=tableInfos;
			len$=tableInfos.length;

			for(i$=0;i$<len$;++i$){
				tableInfo=arr$[i$];
				if(sb.length()>0){
					sb.append(",");
				}

				if(tableInfo.getProjectName()!=null){
					sb.append(tableInfo.getProjectName());
				}else{
					sb.append(this.odps.getDefaultProject());
				}

				sb.append(".");
				sb.append(tableInfo.getTableName());
				parts=tableInfo.getPartPath();
				if(parts!=null&&!parts.trim().isEmpty()){
					sb.append("/");
					if(parts.endsWith("/")){
						parts=parts.substring(0,parts.length()-1);
					}

					sb.append(parts);
				}
			}
		}

		if(sb.length()>0){
			System.err.println("\t"+sb.toString());
			sb.delete(0,sb.length());
		}

		System.err.println("Outputs:");
		tableInfos=OutputUtils.getTables(this.conf);
		if(tableInfos!=null){
			arr$=tableInfos;
			len$=tableInfos.length;

			for(i$=0;i$<len$;++i$){
				tableInfo=arr$[i$];
				if(sb.length()>0){
					sb.append(",");
				}

				if(tableInfo.getProjectName()!=null){
					sb.append(tableInfo.getProjectName());
				}else{
					sb.append(this.odps.getDefaultProject());
				}

				sb.append(".");
				sb.append(tableInfo.getTableName());
				parts=tableInfo.getPartPath();
				if(parts!=null&&!parts.trim().isEmpty()){
					sb.append("/");
					if(parts.endsWith("/")){
						parts=parts.substring(0,parts.length()-1);
					}

					sb.append(parts);
				}
			}
		}

		if(sb.length()>0){
			System.err.println("\t"+sb.toString());
			sb.delete(0,sb.length());
		}

	}

	private void printStageStatic(){
		StringBuilder sb=new StringBuilder();
		Iterator i$=this.stageStaticList.iterator();

		while(i$.hasNext()){
			StageStatic item=(StageStatic)i$.next();
			sb.append("\n"+item.getTaskId());
			sb.append("\n\tWorker Count: "+item.getWorkerCount());
			sb.append("\n\tInput Records: ");
			sb.append("\n\t\tinput: ");
			sb.append(item.getTotalInputRecords());
			sb.append(" (min: ");
			sb.append(item.getMinInputRecords());
			sb.append(", max: ");
			sb.append(item.getMaxInputRecords());
			sb.append(", avg: ");
			sb.append(item.getAvgInputRecords());
			sb.append(")");
			sb.append("\n\tOutput Records: ");
			sb.append("\n\t\t");
			sb.append(item.getNextTaskId());
			sb.append(": ");
			sb.append(item.getTotalOutputRecords());
			sb.append(" (min: ");
			sb.append(item.getMinOutputRecords());
			sb.append(", max: ");
			sb.append(item.getMaxOutputRecords());
			sb.append(", avg: ");
			sb.append(item.getAvgOutputRecords());
			sb.append(")");
		}

		System.err.println(sb.toString());
	}

	private void printCounters(){
		int totalCount=0;
		int frameWorkCounterCount=0;
		int jobCounterCount=0;
		int userCounterCount=0;
		Iterator sb=this.counters.iterator();

		while(sb.hasNext()){
			CounterGroup i$=(CounterGroup)sb.next();
			Iterator group=i$.iterator();

			while(group.hasNext()){
				Counter i$1=(Counter)group.next();
				if(!i$1.getDisplayName().startsWith("__EMPTY_")){
					if(i$.getDisplayName().equals(JobCounter.class.getName())){
						++frameWorkCounterCount;
					}else if(i$.getDisplayName().equals("com.aliyun.odps.mapred.local.Counter.JobCounter")){
						++jobCounterCount;
					}else{
						++userCounterCount;
					}

					++totalCount;
				}
			}
		}

		StringBuilder var10=new StringBuilder("Counters: "+totalCount);
		var10.append("\n\tMap-Reduce Framework: "+frameWorkCounterCount);
		Iterator var11=this.counters.iterator();

		while(true){
			Counter counter;
			CounterGroup var12;
			Iterator var13;
			do{
				if(!var11.hasNext()){
					var10.append("\n\tUser Defined Counters: "+userCounterCount);
					var11=this.counters.iterator();

					while(true){
						do{
							do{
								if(!var11.hasNext()){
									System.err.println(var10.toString().toLowerCase());
									return;
								}

								var12=(CounterGroup)var11.next();
							}
							while(var12.getDisplayName().equals(JobCounter.class.getName()));
						}
						while(var12.getDisplayName().equals("com.aliyun.odps.mapred.local.Counter.JobCounter"));

						var10.append("\n\t\t"+var12.getDisplayName());
						var13=var12.iterator();

						while(var13.hasNext()){
							counter=(Counter)var13.next();
							if(!counter.getDisplayName().equals(JobCounter.__EMPTY_WILL_NOT_SHOW.toString())){
								var10.append("\n\t\t\t"+counter.getDisplayName()+"="+counter.getValue());
							}
						}
					}
				}

				var12=(CounterGroup)var11.next();
			}while(!var12.getDisplayName().equals(JobCounter.class.getName()));

			var13=var12.iterator();

			while(var13.hasNext()){
				counter=(Counter)var13.next();
				if(!counter.getDisplayName().startsWith("__EMPTY_")){
					var10.append("\n\t\t"+counter.getDisplayName()+"="+counter.getValue());
				}
			}
		}
	}

	public Configuration getConf(){
		return this.conf;
	}

	public void setConf(Configuration conf){
		this.conf=new LocalConf((JobConf)conf);
	}
}
