package com.aliyun.odps.mapred;

import com.aliyun.odps.Column;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.TableInfo;
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
import com.aliyun.odps.mapred.local.utils.LocalRunUtils;
import com.aliyun.odps.mapred.local.utils.PartitionUtils;
import com.aliyun.odps.mapred.local.utils.SchemaUtils;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.pipeline.Pipeline;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.csvreader.CsvReader;
import com.google.code.externalsorting.ExternalSort;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

public class JobRunnerImpl implements JobRunner{
	private List<FileSplit> inputs;
	private WareHouse wareHouse;
	private JobDirecotry jobDirecotry;
	private Counters counters;
	private Odps odps;
	private LocalConf conf;
	private Map<FileSplit,TableInfo> splitToTableInfo;
	private List<StageStatic> stageStaticList;
	public static Counter EMPTY_COUNTER;
	private Pipeline pipeline;

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
		this.jobDirecotry.writeJobConf();
		this.processInputs();
		this.processOutputs();
		this.fillTableInfo();
		this.handleNonPipeMode();
		this.moveOutputs();

		try{
			if(!this.conf.isRetainTempData()){
				FileUtils.deleteDirectory(this.jobDirecotry.getJobDir());
			}
		}catch(Exception var3){
		}
		this.mergeResults();
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
		LocalTaskId taskId=new LocalTaskId("M1",0,"demo");
		StageStatic stageStatic=this.createStageStatic(taskId);
		stageStatic.setWorkerCount(var9);

		int reduceId;
		TaskId var11;
		for(reduceId=0;reduceId<var9;++reduceId){
			FileSplit reduceDriver=this.inputs.size()>0?(FileSplit)this.inputs.get(reduceId):FileSplit.NullSplit;
			var11=new TaskId("M",reduceId+1);
			MapDriver mapDriver=new MapDriver(this.conf,reduceDriver,var11,buffer,this.counters,(TableInfo)this.splitToTableInfo.get(reduceDriver));
			mapDriver.run();
			this.setInputOutputRecordCount(stageStatic);
		}

		if(var10>0){
			taskId=new LocalTaskId("R2_1",0,"demo");
			stageStatic.setNextTaskId("R2_1");
			stageStatic=this.createStageStatic(taskId);
			stageStatic.setWorkerCount(var10);

			for(reduceId=0;reduceId<var10;++reduceId){
				var11=new TaskId("R",reduceId);
				ReduceDriver var12=new ReduceDriver(this.conf,buffer,(MapOutputBuffer)null,var11,this.counters,reduceId);
				var12.run();
				this.setInputOutputRecordCount(stageStatic);
			}

			stageStatic.setNextTaskId("R2_1FS_9");
		}else{
			stageStatic.setNextTaskId("M1");
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
		if(tableInfo!=null&&!StringUtils.isBlank(tableInfo.getTableName())){
			tableInfo.setProjectName("demo");
			File tableDir=this.wareHouse.getInstance().getTableDir(tableInfo.getProjectName(),tableInfo.getTableName());
			tableDir.mkdirs();
			FileUtils.cleanDirectory(tableDir);
			File schemaFile=new File(tableDir,"__schema__");
			if(!schemaFile.exists()){
				StringBuffer sb=new StringBuffer();
				sb.append("project="+tableInfo.getProjectName());
				sb.append("\n");
				sb.append("table="+tableInfo.getTableName());
				sb.append("\n");
				sb.append("columns=");
				sb.append("word:STRING");
				sb.append("\n");
				FileOutputStream fout=new FileOutputStream(schemaFile);
				String exception=sb.toString();
				fout.write(exception.getBytes("utf-8"));
				fout.close();
			}
			File inFile=new File(tableInfo.getTableName());
			File outFile;
			int num_part=0;
			BufferedReader reader=null;
			try{
				outFile=new File(tableDir,"data"+num_part);
				FileOutputStream fout=new FileOutputStream(outFile);
				reader=new BufferedReader(new FileReader(inFile));
				String line;
				int lineNum=0;
				CsvReader csvReader=new CsvReader(tableInfo.getTableName());
				while(csvReader.readRecord()){
					String[] values=csvReader.getValues();
					for(String word : values){
						fout.write((word+"\n").getBytes());
						lineNum++;
						if(lineNum==1638400){
							lineNum=0;
							++num_part;
							fout.close();
							outFile=new File(tableDir,"data"+num_part);
							fout=new FileOutputStream(outFile);
						}
					}
				}
				reader.close();
				fout.close();
			}catch(IOException e){
				e.printStackTrace();
			}finally{
				if(reader!=null){
					try{
						reader.close();
					}catch(IOException e1){
					}
				}
			}

			TableMeta whTblMeta=this.wareHouse.getTableMeta(tableInfo.getProjectName(),tableInfo.getTableName());
			Column[] whReadFields=whTblMeta.getCols();
			File file;
			if(tableInfo.getPartSpec()!=null&&tableInfo.getPartSpec().size()>0){
				throw new IOException("ODPS-0720121: Invalid table partSpectable "+tableInfo.getProjectName()+"."+tableInfo.getTableName()+" is not partitioned table");
			}

			File whSrcDir1=this.wareHouse.getTableDir(whTblMeta.getProjName(),whTblMeta.getTableName());
			if(LocalRunUtils.listDataFiles(whSrcDir1).size()>0){
				File tempDataDir1=this.jobDirecotry.getInputDir(this.wareHouse.getRelativePath(whTblMeta.getProjName(),whTblMeta.getTableName(),(PartitionSpec)null,new Object[0]));
				this.wareHouse.copyTable(whTblMeta.getProjName(),whTblMeta.getTableName(),(PartitionSpec)null,null,tempDataDir1,this.conf.getLimitDownloadRecordCount(),this.conf.getInputColumnSeperator());
				Iterator i$1=LocalRunUtils.listDataFiles(tempDataDir1).iterator();

				while(i$1.hasNext()){
					file=(File)i$1.next();
					FileSplit split2=new FileSplit(file,whReadFields,0L,file.length());
					this.splitToTableInfo.put(split2,tableInfo);
					this.inputs.add(split2);
				}
			}

		}else{
			throw new RuntimeException("Invalid TableInfo: "+tableInfo);
		}
	}

	private void processInputs() throws IOException, OdpsException{
		TableInfo[] inputTableInfos=InputUtils.getTables(this.conf);
		if(inputTableInfos==null){
		}else{
			TableInfo[] arr$=inputTableInfos;
			int len$=inputTableInfos.length;

			for(int i$=0;i$<len$;++i$){
				TableInfo tableInfo=arr$[i$];
				this.processInput(tableInfo);
			}

			if(this.inputs.isEmpty()){
				this.inputs.add(FileSplit.NullSplit);
			}

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
				File tableDir=this.wareHouse.getInstance().getTableDir(tableInfo.getProjectName(),tableInfo.getTableName());
				tableDir.mkdirs();
				FileUtils.cleanDirectory(tableDir);
				File schemaFile=new File(tableDir,"__schema__");
				if(!schemaFile.exists()){
					StringBuffer sb=new StringBuffer();
					sb.append("project="+tableInfo.getProjectName());
					sb.append("\n");
					sb.append("table="+tableInfo.getTableName());
					sb.append("\n");
					sb.append("columns=word:STRING,count:BIGINT");
					sb.append("\n");
					//LOG.info("generate schema file: " + schemaFile.getAbsolutePath());
					FileOutputStream fout=new FileOutputStream(schemaFile);
					String exception=sb.toString();
					fout.write(exception.getBytes("utf-8"));
					fout.close();
				}
				if(this.wareHouse.existsTable(tableInfo.getProjectName(),tableInfo.getTableName())){
					tblMeta=this.wareHouse.getTableMeta(tableInfo.getProjectName(),tableInfo.getTableName());
				}else{

					File tableDirInWarehouse=this.wareHouse.getTableDir(tableInfo.getProjectName(),tableInfo.getTableName());
					tableDirInWarehouse.mkdirs();
					SchemaUtils.generateSchemaFile(tblMeta,(List)null,tableDirInWarehouse);
				}
				SchemaUtils.generateSchemaFile(tblMeta,(List)null,tableDirInJobDir);
				this.conf.setOutputSchema(tblMeta.getCols(),tableInfo.getLabel());
			}
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
					LocalRunUtils.removeDataFiles(whOutputDir);
					this.wareHouse.copyDataFiles(tempTblDir,(List)null,whOutputDir,this.conf.getInputColumnSeperator());
				}else{
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
			this.conf.setNumReduceTasks(reduceNum1);
			return reduceNum1;
		}
	}

	public Configuration getConf(){
		return this.conf;
	}

	public void setConf(Configuration conf){
		this.conf=new LocalConf((JobConf)conf);
	}

	private void mergeResults() throws IOException{
		TableInfo[] outputs=OutputUtils.getTables(this.conf);
		TableInfo[] arr$=outputs;
		int len$=outputs.length;
		for(int i$=0;i$<len$;++i$){
			TableInfo tableInfo=arr$[i$];
			File tableDir=this.wareHouse.getInstance().getTableDir(tableInfo.getProjectName(),tableInfo.getTableName());
			tableDir.mkdirs();
			File outFile=new File(tableInfo.getTableName()+".tmp");
			FileOutputStream fout=new FileOutputStream(outFile);
			BufferedReader reader=null;
			File[] files=tableDir.listFiles();
			int numFile=files.length;
			for(int j$=0;j$<numFile;++j$){
				if(!files[j$].getName().equals("__schema__")&&!files[j$].getName().equals(tableInfo.getTableName())){
					try{
						reader=new BufferedReader(new FileReader(files[j$]));
						String line;
						while((line=reader.readLine())!=null){
							line=line+"\n";
							fout.write(line.getBytes("utf-8"));
						}
						reader.close();
					}catch(IOException e){
						e.printStackTrace();
					}finally{
						if(reader!=null){
							try{
								reader.close();
							}catch(IOException e1){
							}
						}
					}
				}
			}
			fout.close();
			File output=new File(tableInfo.getTableName());
			outFile.deleteOnExit();
			ExternalSort.sort(outFile,output);
		}
	}
}
