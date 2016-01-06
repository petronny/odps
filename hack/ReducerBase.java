package com.aliyun.odps.mapred;

import com.aliyun.odps.OdpsDeprecatedLogger;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.ReducerBase$AjcClosure1;
import com.aliyun.odps.mapred.ReducerBase$AjcClosure3;
import com.aliyun.odps.mapred.ReducerBase$AjcClosure5;
import com.aliyun.odps.mapred.Reducer.TaskContext;
import java.io.IOException;
import java.util.Iterator;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.JoinPoint.StaticPart;
import org.aspectj.runtime.reflect.Factory;

public class ReducerBase implements Reducer {
        public ReducerBase() {
        }

        public void setup(TaskContext context) throws IOException {
        }

        public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        }

        public void cleanup(TaskContext context) throws IOException {
        }

        /** @deprecated */
        @Deprecated
        public void setup(com.aliyun.odps.mapred.TaskContext context) throws IOException {
                JoinPoint var3 = Factory.makeJP(ajc$tjp_0, this, this, context);
                OdpsDeprecatedLogger var10000 = OdpsDeprecatedLogger.aspectOf();
                Object[] var4 = new Object[]{this, context, var3};
                var10000.around((new ReducerBase$AjcClosure1(var4)).linkClosureAndJoinPoint(69648));
        }

        /** @deprecated */
        @Deprecated
        public void reduce(Record key, Iterator<Record> values, com.aliyun.odps.mapred.TaskContext context) throws IOException {
                StaticPart var10000 = ajc$tjp_1;
                Object[] var8 = new Object[]{key, values, context};
                JoinPoint var7 = Factory.makeJP(var10000, this, this, var8);
                OdpsDeprecatedLogger var10 = OdpsDeprecatedLogger.aspectOf();
                Object[] var9 = new Object[]{this, key, values, context, var7};
                var10.around((new ReducerBase$AjcClosure3(var9)).linkClosureAndJoinPoint(69648));
        }

        /** @deprecated */
        @Deprecated
        public void cleanup(com.aliyun.odps.mapred.TaskContext context) throws IOException {
                JoinPoint var3 = Factory.makeJP(ajc$tjp_2, this, this, context);
                OdpsDeprecatedLogger var10000 = OdpsDeprecatedLogger.aspectOf();
                Object[] var4 = new Object[]{this, context, var3};
                var10000.around((new ReducerBase$AjcClosure5(var4)).linkClosureAndJoinPoint(69648));
        }

        static {
                ajc$preClinit();
        }
}
