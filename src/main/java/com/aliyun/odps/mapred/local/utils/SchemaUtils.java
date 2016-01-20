package com.aliyun.odps.mapred.local.utils;

/**
 * Created by petron on 16-1-17.
 */

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.local.TableMeta;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SchemaUtils {
	private static final Log LOG = LogFactory.getLog(JobClient.class);

	public SchemaUtils() {
	}

	public static boolean existsSchemaFile(File dir) {
		return (new File(dir, "__schema__")).exists();
	}

	public static void generateSchemaFile(TableMeta table, List<Integer> indexes, File dir) {
		if(table != null && dir != null) {
			if(!StringUtils.isBlank(table.getProjName()) && !StringUtils.isBlank(table.getTableName())) {
				TableInfo tableInfo = TableInfo.builder().projectName(table.getProjName()).tableName(table.getTableName()).build();
				StringBuffer sb = new StringBuffer();
				sb.append("project=" + table.getProjName());
				sb.append("\n");
				sb.append("table=" + table.getTableName());
				sb.append("\n");
				StringBuffer sb1 = new StringBuffer();
				Column[] columns = table.getCols();
				int length = indexes == null?table.getCols().length:indexes.size();

				int schemaFile;
				for(int partitions = 0; partitions < length; ++partitions) {
					schemaFile = indexes == null?partitions:((Integer)indexes.get(partitions)).intValue();
					Column out = columns[schemaFile];
					if(sb1.length() > 0) {
						sb1.append(",");
					}

					sb1.append(out.getName() + ":" + out.getType().toString());
				}

				sb.append("columns=" + sb1.toString());
				sb.append("\n");
				Column[] var21 = table.getPartitions();
				if(var21 != null && var21.length > 0) {
					sb1 = new StringBuffer();

					for(schemaFile = 0; schemaFile < var21.length; ++schemaFile) {
						if(sb1.length() > 0) {
							sb1.append(",");
						}

						sb1.append(var21[schemaFile].getName() + ":" + var21[schemaFile].getType().toString());
					}

					sb.append("partitions=" + sb1.toString());
					sb.append("\n");
				}

				dir.mkdirs();
				File var22 = new File(dir, "__schema__");
				FileOutputStream var23 = null;

				try {
					var23 = new FileOutputStream(var22);
					String exception = sb.toString();
					exception = exception.substring(0, exception.length() - 1);
					var23.write(exception.getBytes("utf-8"));
				} catch (IOException var19) {
					throw new RuntimeException(var19);
				} finally {
					try {
						var23.close();
					} catch (IOException var18) {
						throw new RuntimeException(var18);
					}
				}

			} else {
				throw new IllegalArgumentException("Project|table is empty when table.getProjName()|table.getTableName()");
			}
		} else {
			throw new IllegalArgumentException("Missing arguments: table|dir");
		}
	}

	public static TableMeta readSchema(File dir) {
		if(dir != null && dir.exists()) {
			File schemaFile = new File(dir, "__schema__");
			BufferedReader br = null;

			try {
				br = new BufferedReader(new InputStreamReader(new FileInputStream(schemaFile)));
			} catch (FileNotFoundException var18) {
				throw new RuntimeException("__schema__ file not exists in direcotry " + dir.getAbsolutePath());
			}

			String line = null;

			try {
				line = br.readLine();
			} catch (IOException var17) {
				throw new RuntimeException(var17);
			}

			String project = null;
			String table = null;
			ArrayList cols = new ArrayList();
			ArrayList parts = new ArrayList();

			while(true) {
				while(true) {
					while(line != null) {
						line = line.trim();
						if(!line.equals("") && !line.startsWith("#")) {
							String[] e = line.split("=");
							if(e != null && e.length == 2 && e[0] != null && !e[0].trim().isEmpty() && e[1] != null && !e[1].trim().isEmpty()) {
								e[0] = e[0].trim();
								e[1] = e[1].trim();
								if(e[0].equals("project")) {
									project = e[1];
								} else if(e[0].equals("table")) {
									table = e[1];
									if(table == null || table.trim().isEmpty()) {
										throw new RuntimeException("Table schema file \'_schema_\' must include \'table\'");
									}
								} else {
									String e1;
									String[] ss;
									int i;
									String[] temp;
									if(e[0].equals("columns")) {
										e1 = e[1];
										if(e1 == null || e1.trim().isEmpty()) {
											throw new RuntimeException("Table schema file \'_schema_\' must include \'columns\'");
										}

										ss = e1.split(",");

										for(i = 0; i < ss.length; ++i) {
											temp = ss[i].trim().split(":");
											if(temp.length == 2) {
												temp[0] = temp[0].trim();
												temp[1] = temp[1].trim();
												if(!temp[0].isEmpty() && !temp[1].isEmpty()) {
													cols.add(new Column(temp[0], OdpsType.valueOf(temp[1].toUpperCase())));
												}
											}
										}

										if(cols.size() == 0) {
											throw new RuntimeException("\'columns\' in table schema file \'_schema_\' has invalid value");
										}
									} else if(e[0].equals("partitions")) {
										e1 = e[1];
										if(e1 != null && !e1.trim().isEmpty()) {
											ss = e1.split(",");

											for(i = 0; i < ss.length; ++i) {
												temp = ss[i].trim().split(":");
												if(temp.length == 2) {
													temp[0] = temp[0].trim();
													temp[1] = temp[1].trim();
													if(!temp[0].isEmpty() && !temp[1].isEmpty()) {
														parts.add(new Column(temp[0], OdpsType.valueOf(temp[1].toUpperCase())));
													}
												}
											}
										}
									}
								}

								try {
									line = br.readLine();
								} catch (IOException var14) {
									throw new RuntimeException(var14);
								}
							} else {
								try {
									line = br.readLine();
								} catch (IOException var15) {
									throw new RuntimeException(var15);
								}
							}
						} else {
							try {
								line = br.readLine();
							} catch (IOException var16) {
								throw new RuntimeException(var16);
							}
						}
					}

					try {
						br.close();
					} catch (IOException var13) {
						throw new RuntimeException(var13);
					}

					return new TableMeta(project, table, (Column[])cols.toArray(new Column[cols.size()]), (Column[])parts.toArray(new Column[parts.size()]));
				}
			}
		} else {
			return null;
		}
	}

	public static String[] getColumnNames(Column[] cols) {
		String[] names = new String[cols.length];

		for(int i = 0; i < cols.length; ++i) {
			names[i] = cols[i].getName();
		}

		return names;
	}
}
