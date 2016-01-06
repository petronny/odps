package com.aliyun.odps.mapred;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import java.io.IOException;

public interface Mapper {
        void setup(Mapper.TaskContext var1) throws IOException;

        void map(long var1, Record var3, Mapper.TaskContext var4) throws IOException;

        void cleanup(Mapper.TaskContext var1) throws IOException;

        public interface TaskContext extends com.aliyun.odps.mapred.TaskContext {
                long getCurrentRecordNum();

                Record getCurrentRecord();

                boolean nextRecord();

                TableInfo getInputTableInfo();
        }
}
