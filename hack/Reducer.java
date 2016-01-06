package com.aliyun.odps.mapred;

import com.aliyun.odps.data.Record;
import java.io.IOException;
import java.util.Iterator;

public interface Reducer {
        void setup(Reducer.TaskContext var1) throws IOException;

        void reduce(Record var1, Iterator<Record> var2, Reducer.TaskContext var3) throws IOException;

        void cleanup(Reducer.TaskContext var1) throws IOException;

        public interface TaskContext extends com.aliyun.odps.mapred.TaskContext {
                boolean nextKeyValue();

                Record getCurrentKey();

                Iterator<Record> getValues();
        }
}
