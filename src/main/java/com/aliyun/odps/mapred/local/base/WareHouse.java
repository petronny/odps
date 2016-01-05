package com.aliyun.odps.mapred.local.base;

/**
 * Created by petron on 16-1-5.
 */

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.local.TableMeta;
import com.aliyun.odps.mapred.local.common.Constants;
import com.aliyun.odps.mapred.local.utils.CommonUtils;
import com.aliyun.odps.mapred.local.utils.DownloadUtils;
import com.aliyun.odps.mapred.local.utils.LocalRunUtils;
import com.aliyun.odps.mapred.local.utils.PartitionUtils;
import com.aliyun.odps.mapred.local.utils.SchemaUtils;
import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class WareHouse{
	private static final Log LOG=LogFactory.getLog(WareHouse.class);
	private File warehouseDir=new File("warehouse");
	private static WareHouse wareHouse;
	private ThreadLocal<Odps> odpsThreadLocal=new ThreadLocal();

	private WareHouse(){
		if(!this.warehouseDir.exists()){
			this.warehouseDir.mkdirs();
		}

	}

	public static WareHouse getInstance(){
		if(wareHouse==null){
			Class var0=WareHouse.class;
			synchronized(WareHouse.class){
				if(wareHouse==null){
					wareHouse=new WareHouse();
				}
			}
		}

		return wareHouse;
	}

	public File getWarehouseDir(){
		return this.warehouseDir;
	}

	public File getProjectDir(String projName){
		return new File(this.warehouseDir,projName);
	}

	public File getTableDir(String projName,String tblName){
		File tableDir=new File(this.getProjectDir(projName),"__tables__"+File.separator+tblName);
		if(!tableDir.exists()){
			File oldVersionDir=new File(this.getProjectDir(projName),projName+File.separator+tblName);
			if(oldVersionDir.exists()){
				return oldVersionDir;
			}
		}

		return tableDir;
	}

	public File getPartitionDir(String projName,String tblName,PartitionSpec partSpec){
		if(partSpec!=null&&!partSpec.isEmpty()){
			Map map=this.getPartitionToPathMap(projName,tblName);
			Iterator i$=map.keySet().iterator();

			PartitionSpec key;
			do{
				if(!i$.hasNext()){
					return null;
				}

				key=(PartitionSpec)i$.next();
			}while(!PartitionUtils.isEqual(key,partSpec));

			return (File)map.get(key);
		}else{
			return this.getTableDir(projName,tblName);
		}
	}

	public File getReourceFile(String projName,String resourceName){
		return new File(this.warehouseDir,projName+File.separator+"__resources__"+File.separator+resourceName);
	}

	public File getTableReourceFile(String projName,String resourceName){
		File tableResourceDir=this.getReourceFile(projName,resourceName);
		return tableResourceDir.listFiles()[0];
	}

	public File getTableSchemeFile(String projectName,String tableName) throws FileNotFoundException{
		if(!this.existsTableSchema(projectName,tableName)){
			throw new FileNotFoundException("Table Directory :"+projectName+"."+tableName+" Not exists in warehouse!");
		}else{
			File tableDir=this.getTableDir(projectName,tableName);
			return new File(tableDir,"__schema__");
		}
	}

	public String getRelativePath(String projName,String tblName,PartitionSpec partSpec,Object... flag){
		String relativePath=projName+File.separator+tblName+File.separator;
		if(partSpec!=null){
			relativePath=relativePath+PartitionUtils.toString(partSpec);
		}

		return relativePath;
	}

	public List<File> getDataFiles(String projName,String tblName,PartitionSpec pattern,char inputColumnSeperator) throws IOException, OdpsException{
		if(pattern!=null&&!pattern.isEmpty()&&!this.existsPartition(projName,tblName,pattern)){
			LinkedHashMap tableDir1=PartitionUtils.convert(pattern);
			TableInfo tableMeta=TableInfo.builder().projectName(projName).tableName(tblName).partSpec(tableDir1).build();
			DownloadUtils.downloadTableSchemeAndData(getInstance().getOdps(),tableMeta,100,inputColumnSeperator);
		}else if(!this.existsTable(projName,tblName)){
			TableInfo tableDir=TableInfo.builder().projectName(projName).tableName(tblName).build();
			DownloadUtils.downloadTableSchemeAndData(getInstance().getOdps(),tableDir,100,inputColumnSeperator);
		}

		File tableDir2=this.getTableDir(projName,tblName);
		TableMeta tableMeta1=SchemaUtils.readSchema(tableDir2);
		boolean isPartitionTable=false;
		if(tableMeta1.getPartitions()!=null&&tableMeta1.getPartitions().length>0){
			isPartitionTable=true;
		}

		if(!isPartitionTable){
			if(pattern!=null&&!pattern.isEmpty()){
				throw new OdpsException("Table "+projName+"."+tblName+" is not a partition table");
			}else{
				return LocalRunUtils.listDataFiles(tableDir2);
			}
		}else{
			ArrayList result;
			if(pattern==null){
				result=new ArrayList();
				LocalRunUtils.listAllDataFiles(tableDir2,result);
				return result;
			}else{
				result=new ArrayList();
				Map partitionToPathMap=wareHouse.getPartitionToPathMap(projName,tblName);
				Iterator i$=partitionToPathMap.keySet().iterator();

				while(i$.hasNext()){
					PartitionSpec parts=(PartitionSpec)i$.next();
					if(PartitionUtils.match(pattern,parts)){
						result.addAll(LocalRunUtils.listDataFiles((File)partitionToPathMap.get(parts)));
					}
				}

				return result;
			}
		}
	}

	public boolean copyTableSchema(String projectName,String tableName,File destDir,int limitDownloadRecordCount,char inputColumnSeperator) throws IOException, OdpsException{
		if(!StringUtils.isBlank(projectName)&&!StringUtils.isBlank(tableName)&&destDir!=null){
			TableInfo tableInfo=TableInfo.builder().projectName(projectName).tableName(tableName).build();
			LOG.info("Start to copy table schema: "+tableInfo+"-->"+destDir.getAbsolutePath());
			if(!this.existsTable(projectName,tableName)){
				DownloadUtils.downloadTableSchemeAndData(this.getOdps(),tableInfo,limitDownloadRecordCount,inputColumnSeperator);
			}

			File tableDir=this.getTableDir(projectName,tableName);
			File schemaFile=new File(tableDir,"__schema__");
			if(!schemaFile.exists()){
				throw new FileNotFoundException("Schema file of table "+projectName+"."+tableName+" not exists in warehouse.");
			}else{
				if(!destDir.exists()){
					destDir.mkdirs();
				}

				FileUtils.copyFileToDirectory(schemaFile,destDir);
				LOG.info("Finished copy table schema: "+tableInfo+"-->"+destDir.getAbsolutePath());
				return true;
			}
		}else{
			return false;
		}
	}

	public boolean copyTable(String projectName,String tableName,PartitionSpec partSpec,String[] readCols,File destDir,int limitDownloadRecordCount,char inputColumnSeperator){
		if(!StringUtils.isBlank(projectName)&&!StringUtils.isBlank(tableName)&&destDir!=null){
			TableInfo tableInfo=TableInfo.builder().projectName(projectName).tableName(tableName).partSpec(partSpec).build();
			LOG.info("Start to copy table: "+tableInfo+"-->"+destDir.getAbsolutePath());
			boolean hasPartition=false;
			if(partSpec!=null&&!partSpec.isEmpty()){
				hasPartition=true;
			}

			if(hasPartition&&!this.existsPartition(projectName,tableName,partSpec)){
				DownloadUtils.downloadTableSchemeAndData(this.getOdps(),tableInfo,limitDownloadRecordCount,inputColumnSeperator);
			}else if(!this.existsTable(projectName,tableName)){
				DownloadUtils.downloadTableSchemeAndData(this.getOdps(),tableInfo,limitDownloadRecordCount,inputColumnSeperator);
			}

			File whTableDir=this.getTableDir(projectName,tableName);
			File schemaFile=new File(whTableDir,"__schema__");
			if(!schemaFile.exists()){
				throw new RuntimeException("Schema file of table "+projectName+"."+tableName+" not exists in warehouse.");
			}else{
				if(!destDir.exists()){
					destDir.mkdirs();
				}

				try{
					FileUtils.copyFileToDirectory(schemaFile,destDir);
				}catch(IOException var24){
					throw new RuntimeException("Copy schema file of table "+tableInfo+" failed!"+var24.getMessage());
				}

				TableMeta tableMeta=this.getTableMeta(projectName,tableName);
				List indexes=LocalRunUtils.genReadColsIndexes(tableMeta,readCols);
				if(hasPartition){
					Collection e=FileUtils.listFiles(whTableDir,HiddenFileFilter.VISIBLE,HiddenFileFilter.VISIBLE);
					Iterator i$=e.iterator();

					while(i$.hasNext()){
						File dataFile=(File)i$.next();
						if(!dataFile.getName().equals("__schema__")){
							String parentDir=dataFile.getParentFile().getAbsolutePath();
							String partPath=parentDir.substring(whTableDir.getAbsolutePath().length(),parentDir.length());
							PartitionSpec ps=PartitionUtils.convert(partPath);
							if(PartitionUtils.isEqual(ps,partSpec)){
								File destPartitionDir=new File(destDir,PartitionUtils.toString(ps));
								destPartitionDir.mkdirs();

								try{
									this.copyDataFiles(dataFile.getParentFile(),indexes,destPartitionDir,inputColumnSeperator);
								}catch(IOException var23){
									throw new RuntimeException("Copy data file of table "+tableInfo+" failed!"+var23.getMessage());
								}
							}
						}
					}
				}else{
					try{
						this.copyDataFiles(whTableDir,indexes,destDir,inputColumnSeperator);
					}catch(IOException var22){
						throw new RuntimeException("Copy data file of table "+tableInfo+" failed!"+var22.getMessage());
					}
				}

				LOG.info("Finished copy table: "+tableInfo+"-->"+destDir.getAbsolutePath());
				return true;
			}
		}else{
			return false;
		}
	}

	public void copyResource(String projName,String resourceName,File resourceRootDir,int limitDownloadRecordCount,char inputColumnSeperator) throws IOException, OdpsException{
		if(!StringUtils.isBlank(projName)&&!StringUtils.isBlank(resourceName)&&resourceRootDir!=null){
			if(!resourceRootDir.exists()){
				resourceRootDir.mkdirs();
			}

			LOG.info("Start to copy resource: "+projName+"."+resourceName+"-->"+resourceRootDir.getAbsolutePath());
			if(!this.existsResource(projName,resourceName)){
				DownloadUtils.downloadResource(this.getOdps(),projName,resourceName,limitDownloadRecordCount,inputColumnSeperator);
			}

			File file=this.getReourceFile(projName,resourceName);
			if(file.isDirectory()){
				File tableResourceDir=new File(resourceRootDir,resourceName);
				TableInfo refTableInfo=this.getReferencedTable(projName,resourceName);
				LinkedHashMap partitions=refTableInfo.getPartSpec();
				if(partitions!=null&&partitions.size()>0){
					PartitionSpec partSpec=new PartitionSpec();
					Iterator i$=partitions.keySet().iterator();

					while(i$.hasNext()){
						String key=(String)i$.next();
						partSpec.set(key,(String)partitions.get(key));
					}

					this.copyTable(refTableInfo.getProjectName(),refTableInfo.getTableName(),partSpec,(String[])null,tableResourceDir,limitDownloadRecordCount,inputColumnSeperator);
				}else{
					this.copyTable(refTableInfo.getProjectName(),refTableInfo.getTableName(),(PartitionSpec)null,(String[])null,tableResourceDir,limitDownloadRecordCount,inputColumnSeperator);
				}
			}else{
				if(!this.existsResource(projName,resourceName)){
					DownloadUtils.downloadResource(this.getOdps(),projName,resourceName,limitDownloadRecordCount,inputColumnSeperator);
				}

				FileUtils.copyFileToDirectory(file,resourceRootDir);
			}

			LOG.info("Finished copy resource: "+projName+"."+resourceName+"-->"+resourceRootDir.getAbsolutePath());
		}
	}

	public void copyDataFiles(File srcDir,List<Integer> indexes,File destDir,char inputColumnSeperator) throws IOException{
		Iterator i$;
		File file;
		if(indexes!=null&&!indexes.isEmpty()){
			i$=LocalRunUtils.listDataFiles(srcDir).iterator();

			while(i$.hasNext()){
				file=(File)i$.next();
				CsvReader reader=new CsvReader(file.getAbsolutePath(),inputColumnSeperator,Constants.encoding);
				CsvWriter writer=new CsvWriter((new File(destDir,file.getName())).getAbsolutePath(),inputColumnSeperator,Constants.encoding);

				while(reader.readRecord()){
					String[] vals=reader.getValues();
					String[] newVals=new String[indexes.size()];

					for(int i=0;i<indexes.size();++i){
						newVals[i]=vals[((Integer)indexes.get(i)).intValue()];
					}

					writer.writeRecord(newVals);
				}

				writer.close();
				reader.close();
			}
		}else{
			i$=LocalRunUtils.listDataFiles(srcDir).iterator();

			while(i$.hasNext()){
				file=(File)i$.next();
				FileUtils.copyFileToDirectory(file,destDir);
			}
		}

	}

	public File createPartitionDir(String projName,String tblName,PartitionSpec partSpec){
		File tableDir=this.getTableDir(projName,tblName);
		if(!tableDir.exists()){
			tableDir.mkdirs();
		}

		if(partSpec!=null&&!partSpec.isEmpty()){
			File partitionDir=new File(tableDir,PartitionUtils.toString(partSpec));
			if(!partitionDir.exists()){
				partitionDir.mkdirs();
			}

			return partitionDir;
		}else{
			return tableDir;
		}
	}

	public boolean createTableReourceFile(String projName,String resourceName,TableInfo refTableInfo){
		StringBuffer sb=new StringBuffer();
		sb.append(refTableInfo.getProjectName());
		sb.append(".");
		sb.append(refTableInfo.getTableName());
		String partitions=refTableInfo.getPartPath();
		if(partitions!=null&&!partitions.trim().isEmpty()){
			sb.append("(");
			String tableResourceDir=partitions.replaceAll("/",",");
			if(tableResourceDir.endsWith(",")){
				tableResourceDir=tableResourceDir.substring(0,tableResourceDir.length()-1);
			}

			sb.append(tableResourceDir);
			sb.append(")");
		}

		File tableResourceDir1=this.getReourceFile(projName,resourceName);
		if(!tableResourceDir1.exists()){
			tableResourceDir1.mkdirs();
		}

		PrintWriter pw=null;

		try{
			pw=new PrintWriter(new File(tableResourceDir1,"__ref__"));
			pw.println(sb.toString());
			boolean e=true;
			return e;
		}catch(FileNotFoundException var12){
			;
		}finally{
			if(pw!=null){
				pw.close();
			}

		}

		return false;
	}

	public TableMeta getResourceSchema(String projName,String resourceName) throws IOException{
		File dir=this.getReourceFile(projName,resourceName);
		return SchemaUtils.existsSchemaFile(dir)?SchemaUtils.readSchema(dir):null;
	}

	public TableMeta getTableMeta(String projName,String tblName){
		if(!StringUtils.isBlank(projName)&&!StringUtils.isBlank(tblName)){
			File dir=this.getTableDir(projName,tblName);
			TableMeta meta=SchemaUtils.readSchema(dir);
			if(meta.getProjName()!=null&&!meta.getProjName().equals(projName)){
				throw new RuntimeException("Invalid project name "+meta.getProjName()+" in file \'warehouse"+File.separator+projName+File.separator+tblName+File.separator+"__schema__"+"\'");
			}else if(meta.getTableName()!=null&&!meta.getTableName().equals(tblName)){
				throw new RuntimeException("Invalid table name "+meta.getProjName()+" in file \'warehouse"+File.separator+projName+File.separator+tblName+File.separator+"__schema__"+"\'");
			}else{
				return meta;
			}
		}else{
			return null;
		}
	}

	public Map<PartitionSpec,File> getPartitionToPathMap(String projName,String tblName){
		File tableDir=this.getTableDir(projName,tblName);
		TableMeta tableMeta=SchemaUtils.readSchema(tableDir);
		HashedMap result=new HashedMap();
		File dir=this.getTableDir(projName,tblName);
		Collection dataFiles=FileUtils.listFiles(dir,HiddenFileFilter.VISIBLE,HiddenFileFilter.VISIBLE);
		List emptyPatitions=LocalRunUtils.listEmptyDirectory(dir);
		dataFiles.addAll(emptyPatitions);
		Iterator i$=dataFiles.iterator();

		while(i$.hasNext()){
			File dataFile=(File)i$.next();
			if(!dataFile.getName().equals("__schema__")){
				String partPath=null;
				String ex;
				if(dataFile.isFile()){
					ex=dataFile.getParentFile().getAbsolutePath();
					partPath=ex.substring(dir.getAbsolutePath().length(),ex.length());
				}else{
					ex=dataFile.getAbsolutePath();
					partPath=ex.substring(dir.getAbsolutePath().length(),ex.length());
				}

				try{
					if(partPath.length()>0){
						PartitionSpec ex1=PartitionUtils.convert(partPath);
						if(PartitionUtils.valid(tableMeta.getPartitions(),ex1)){
							result.put(ex1,dataFile.getParentFile());
						}
					}
				}catch(Exception var13){
					;
				}
			}
		}

		return result;
	}

	public List<PartitionSpec> getPartitions(String projName,String tblName){
		Map partitionToPathMap=this.getPartitionToPathMap(projName,tblName);
		ArrayList result=new ArrayList();
		Iterator i$=partitionToPathMap.keySet().iterator();

		while(i$.hasNext()){
			PartitionSpec key=(PartitionSpec)i$.next();
			result.add(key);
		}

		return result;
	}

	public List<String> getProjectNames(){
		File warehouseDir=this.getWarehouseDir();
		if(!warehouseDir.exists()){
			return null;
		}else{
			ArrayList result=new ArrayList();
			File[] projects=warehouseDir.listFiles(new FileFilter(){
				public boolean accept(File pathname){
					return pathname.isDirectory()&&!pathname.isHidden();
				}
			});
			File[] arr$=projects;
			int len$=projects.length;

			for(int i$=0;i$<len$;++i$){
				File p=arr$[i$];
				if(p.isDirectory()){
					result.add(p.getName());
				}
			}

			return result;
		}
	}

	public List<TableMeta> getTableMetas(String projName) throws IOException{
		File projectDir=this.getProjectDir(projName);
		if(!projectDir.exists()){
			return null;
		}else{
			ArrayList result=new ArrayList();
			File[] tables=projectDir.listFiles(new FileFilter(){
				public boolean accept(File pathname){
					return pathname.isDirectory()&&!pathname.isHidden()&&!pathname.getName().equals("__resources__");
				}
			});
			File[] tableBaseDir=tables;
			int arr$=tables.length;

			int len$;
			for(len$=0;len$<arr$;++len$){
				File i$=tableBaseDir[len$];
				if(this.existsTable(projName,i$.getName())){
					TableMeta t=this.getTableMeta(projName,i$.getName());
					if(t!=null){
						result.add(t);
					}
				}
			}

			File var11=new File(projectDir,"__tables__");
			if(!var11.exists()){
				return result;
			}else{
				tables=var11.listFiles(new FileFilter(){
					public boolean accept(File pathname){
						return pathname.isDirectory()&&!pathname.isHidden();
					}
				});
				File[] var12=tables;
				len$=tables.length;

				for(int var13=0;var13<len$;++var13){
					File var14=var12[var13];
					if(this.existsTable(projName,var14.getName())){
						TableMeta tableMeta=this.getTableMeta(projName,var14.getName());
						if(tableMeta!=null){
							result.add(tableMeta);
						}
					}
				}

				return result;
			}
		}
	}

	public List<String> getTableNames(String projName) throws IOException{
		List list=this.getTableMetas(projName);
		if(list!=null&&list.size()!=0){
			ArrayList result=new ArrayList();
			Iterator i$=list.iterator();

			while(i$.hasNext()){
				TableMeta tableMeta=(TableMeta)i$.next();
				result.add(tableMeta.getTableName());
			}

			return result;
		}else{
			return null;
		}
	}

	public TableInfo getReferencedTable(String projName,String resourceName){
		File file=this.getTableReourceFile(projName,resourceName);
		BufferedReader br=null;

		String project;
		try{
			br=new BufferedReader(new InputStreamReader(new FileInputStream(file)));

			String e;
			for(e=br.readLine();e!=null&&(e.trim().isEmpty()||e.startsWith("#"));e=br.readLine()){
				;
			}

			if(e!=null&&!e.trim().isEmpty()){
				int index=e.indexOf(".");
				if(index==-1){
					project=projName;
				}else{
					project=e.substring(0,index);
					e=e.substring(index+1);
				}

				index=e.indexOf("(");
				String table;
				if(index==-1){
					table=e;
					e=null;
				}else{
					table=e.substring(0,index);
					e=e.substring(index+1,e.length()-1);
				}

				String partitions=null;
				if(e!=null){
					partitions="";
					String[] tableInfo=e.split(",");
					String[] arr$=tableInfo;
					int e1=tableInfo.length;

					for(int i$=0;i$<e1;++i$){
						String item=arr$[i$];
						if(!partitions.equals("")){
							partitions=partitions+"/";
						}

						partitions=partitions+item;
					}
				}

				TableInfo var26;
				if(partitions==null){
					var26=TableInfo.builder().projectName(project).tableName(table).build();
				}else{
					var26=TableInfo.builder().projectName(project).tableName(table).partSpec(partitions).build();
				}

				TableInfo var27=var26;
				return var27;
			}

			project=null;
		}catch(IOException var24){
			return null;
		}finally{
			if(br!=null){
				try{
					br.close();
				}catch(IOException var23){
					;
				}
			}

		}

		return null;
	}

	public boolean existsTable(String projName,String tblName){
		return this.existsTableSchema(projName,tblName);
	}

	public boolean existsTableSchema(String projectName,String tableName){
		File tableDir=this.getTableDir(projectName,tableName);
		return !tableDir.exists()?false:(new File(tableDir,"__schema__")).exists();
	}

	public boolean existsPartition(String projectName,String tableName,PartitionSpec partSpec){
		if(!this.existsTable(projectName,tableName)){
			return false;
		}else if(partSpec!=null&&!partSpec.isEmpty()){
			List partionList=this.getPartitions(projectName,tableName);
			if(partionList!=null&&partionList.size()!=0){
				Iterator i$=partionList.iterator();

				PartitionSpec item;
				do{
					if(!i$.hasNext()){
						return false;
					}

					item=(PartitionSpec)i$.next();
				}while(!PartitionUtils.match(partSpec,item));

				return true;
			}else{
				return false;
			}
		}else{
			return true;
		}
	}

	public boolean existsResource(String projName,String resourceName){
		return this.getReourceFile(projName,resourceName).exists();
	}

	public void dropTableIfExists(String projName,String tblName) throws IOException{
		File tableDir=this.getTableDir(projName,tblName);
		if(tableDir!=null&&tableDir.exists()&&tableDir.isDirectory()){
			FileUtils.deleteDirectory(tableDir);
		}

	}

	public void dropResourceIfExists(String projName,String resourceName) throws IOException{
		File resourceDir=this.getReourceFile(projName,resourceName);
		if(resourceDir!=null&&resourceDir.exists()){
			if(resourceDir.isDirectory()){
				FileUtils.deleteDirectory(resourceDir);
			}

			if(resourceDir.isFile()){
				resourceDir.delete();
			}

		}
	}

	public boolean valid(String projName,String tblName,PartitionSpec partitionSpec,String[] readCols) throws OdpsException, IOException{
		if(StringUtils.isBlank(projName)){
			throw new OdpsException("Project "+projName+" is null");
		}else if(!this.existsTable(projName,tblName)){
			throw new OdpsException("table "+projName+"."+tblName+" not exitsts");
		}else if(partitionSpec!=null&&!this.existsPartition(projName,tblName,partitionSpec)){
			throw new OdpsException("table "+projName+"."+tblName+"("+PartitionUtils.toString(partitionSpec)+") not exitsts");
		}else{
			if(readCols!=null){
				TableMeta tableMeta=this.getTableMeta(projName,tblName);
				int columnCount=tableMeta.getCols().length;

				for(int i=0;i<readCols.length;++i){
					boolean isFind=false;

					for(int j=0;j<columnCount;++j){
						if(tableMeta.getCols()[j].getName().equals(readCols[i])){
							isFind=true;
							break;
						}
					}

					if(!isFind){
						throw new OdpsException("table "+projName+"."+tblName+" do not have column :"+readCols[i]);
					}
				}
			}

			return true;
		}
	}

	public List<Object[]> readData(String projName,String tblName,PartitionSpec partitionSpec,String[] readCols,char inputColumnSeperator) throws OdpsException, IOException{
		List dataFiles=this.getDataFiles(projName,tblName,partitionSpec,inputColumnSeperator);
		if(dataFiles!=null&&dataFiles.size()!=0){
			File tableDir=this.getTableDir(projName,tblName);
			TableMeta tableMeta=SchemaUtils.readSchema(tableDir);
			List indexes=LocalRunUtils.genReadColsIndexes(tableMeta,readCols);
			ArrayList result=new ArrayList();
			Iterator i$=dataFiles.iterator();

			while(i$.hasNext()){
				File file=(File)i$.next();

				CsvReader reader;
				Object[] newVals;
				for(reader=new CsvReader(file.getAbsolutePath(),inputColumnSeperator,Constants.encoding);reader.readRecord();result.add(newVals)){
					String[] vals=reader.getValues();
					int i;
					if(indexes!=null&&!indexes.isEmpty()){
						newVals=new Object[indexes.size()];

						for(i=0;i<indexes.size();++i){
							newVals[i]=CommonUtils.fromString(tableMeta.getCols()[((Integer)indexes.get(i)).intValue()].getType(),vals[((Integer)indexes.get(i)).intValue()],"\\N");
						}
					}else{
						newVals=new Object[vals.length];

						for(i=0;i<vals.length;++i){
							newVals[i]=CommonUtils.fromString(tableMeta.getCols()[i].getType(),vals[i],"\\N");
						}
					}
				}

				reader.close();
			}

			return result;
		}else{
			return null;
		}
	}

	public BufferedInputStream readResourceFileAsStream(String project,String resource,char inputColumnSeperator) throws IOException, OdpsException{
		if(!this.existsResource(project,resource)){
			DownloadUtils.downloadResource(getInstance().getOdps(),this.getOdps().getDefaultProject(),resource,100,inputColumnSeperator);
		}

		if(!this.existsResource(project,resource)){
			throw new OdpsException("File Resource "+project+"."+resource+" not exists");
		}else{
			File file=this.getReourceFile(project,resource);
			if(!file.isFile()){
				throw new OdpsException("Resource "+project+"."+resource+" is not a valid file Resource, because it is a direcotry");
			}else{
				return new BufferedInputStream(new FileInputStream(file));
			}
		}
	}

	public byte[] readResourceFile(String project,String resource,char inputColumnSeperator) throws IOException, OdpsException{
		if(!this.existsResource(project,resource)){
			DownloadUtils.downloadResource(getInstance().getOdps(),this.getOdps().getDefaultProject(),resource,100,inputColumnSeperator);
		}

		File file=this.getReourceFile(project,resource);
		if(!file.isFile()){
			throw new OdpsException("Resource "+project+"."+resource+" is not a valid file Resource, because it is a direcotry");
		}else{
			FileInputStream in=new FileInputStream(file);
			ByteArrayOutputStream out=new ByteArrayOutputStream(1024);
			byte[] temp=new byte[1024];

			int length;
			while((length=in.read(temp))!=-1){
				out.write(temp,0,length);
			}

			in.close();
			return out.toByteArray();
		}
	}

	public Iterator<Object[]> readResourceTable(String project,String resource,final char inputColumnSeperator) throws IOException, OdpsException{
		if(!this.existsResource(project,resource)){
			DownloadUtils.downloadResource(getInstance().getOdps(),this.getOdps().getDefaultProject(),resource,100,inputColumnSeperator);
		}

		File tableResourceDir=this.getReourceFile(project,resource);
		if(!tableResourceDir.isDirectory()){
			throw new OdpsException("Resource "+project+"."+resource+" is not a valid file Resource, because it is not a direcotry");
		}else{
			TableInfo tableInfo=this.getReferencedTable(project,resource);
			PartitionSpec partitionSpec=PartitionUtils.convert(tableInfo.getPartSpec());
			final List datafiles=this.getDataFiles(project,tableInfo.getTableName(),partitionSpec,inputColumnSeperator);
			final Column[] schema=SchemaUtils.readSchema(this.getTableDir(project,tableInfo.getTableName())).getCols();
			return new Iterator(){
				CsvReader reader;
				Object[] current;
				boolean fetched;

				public boolean hasNext(){
					if(this.fetched){
						return this.current!=null;
					}else{
						try{
							this.fetch();
						}catch(IOException var2){
							throw new RuntimeException(var2);
						}

						return this.current!=null;
					}
				}

				private void fetch() throws IOException{
					File f;
					if(this.reader==null){
						if(datafiles.isEmpty()){
							this.current=null;
							this.fetched=true;
						}else{
							f=(File)datafiles.remove(0);
							this.reader=new CsvReader(f.getAbsolutePath(),inputColumnSeperator,Constants.encoding);
							this.reader.setSafetySwitch(false);
							this.current=this.read();
							this.fetched=true;
						}
					}else{
						this.current=this.read();
						if(this.current==null&&!datafiles.isEmpty()){
							f=(File)datafiles.remove(0);
							this.reader=new CsvReader(f.getAbsolutePath(),inputColumnSeperator,Constants.encoding);
							this.reader.setSafetySwitch(false);
							this.current=this.read();
							this.fetched=true;
						}else{
							this.fetched=true;
						}
					}
				}

				public Object[] next(){
					if(!this.hasNext()){
						throw new NoSuchElementException();
					}else{
						this.fetched=false;
						return this.current;
					}
				}

				public void remove(){
					throw new UnsupportedOperationException();
				}

				private Object[] read() throws IOException{
					if(!this.reader.readRecord()){
						return null;
					}else{
						String[] vals=this.reader.getValues();
						Object[] result;
						if(vals!=null&&vals.length!=0){
							result=new Object[vals.length];

							for(int i=0;i<vals.length;++i){
								result[i]=CommonUtils.fromString(schema[i].getType(),vals[i],"\\N");
							}
						}else{
							result=null;
						}

						return result;
					}
				}
			};
		}
	}

	public void setOdps(Odps odps){
		this.odpsThreadLocal.remove();
		this.odpsThreadLocal.set(odps);
	}

	public Odps getOdps(){
		return (Odps)this.odpsThreadLocal.get();
	}
}