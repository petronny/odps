package com.aliyun.odps.mapred.local.base;

/**
 * Created by petron on 16-1-5.
 */

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.local.TableMeta;
import com.aliyun.odps.mapred.local.common.Constants;
import com.aliyun.odps.mapred.local.utils.DownloadUtils;
import com.aliyun.odps.mapred.local.utils.LocalRunUtils;
import com.aliyun.odps.mapred.local.utils.PartitionUtils;
import com.aliyun.odps.mapred.local.utils.SchemaUtils;
import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import org.apache.commons.lang.StringUtils;

public class WareHouse{
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
	public String getRelativePath(String projName,String tblName,PartitionSpec partSpec,Object... flag){
		String relativePath=projName+File.separator+tblName+File.separator;
		if(partSpec!=null){
			relativePath=relativePath+PartitionUtils.toString(partSpec);
		}

		return relativePath;
	}


	public boolean copyTable(String projectName,String tableName,PartitionSpec partSpec,String[] readCols,File destDir,int limitDownloadRecordCount,char inputColumnSeperator){
		if(!StringUtils.isBlank(projectName)&&!StringUtils.isBlank(tableName)&&destDir!=null){
			TableInfo tableInfo=TableInfo.builder().projectName(projectName).tableName(tableName).partSpec(partSpec).build();
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

				return true;
			}
		}else{
			return false;
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
	
	public void setOdps(Odps odps){
		this.odpsThreadLocal.remove();
		this.odpsThreadLocal.set(odps);
	}

	public Odps getOdps(){
		return (Odps)this.odpsThreadLocal.get();
	}
}