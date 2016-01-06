package com.aliyun.odps.mapred;

import com.aliyun.odps.OdpsDeprecatedLogger;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.MapperBase$AjcClosure1;
import com.aliyun.odps.mapred.MapperBase$AjcClosure3;
import com.aliyun.odps.mapred.MapperBase$AjcClosure5;
import com.aliyun.odps.mapred.Mapper.TaskContext;
import java.io.IOException;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.JoinPoint.StaticPart;
import org.aspectj.runtime.internal.Conversions;
import org.aspectj.runtime.reflect.Factory;

public class MapperBase implements Mapper {
        public MapperBase() {
        }

        public void setup(TaskContext context) throws IOException {
        }

        public void map(long key, Record record, TaskContext context) throws IOException {
        }

        public void cleanup(TaskContext context) throws IOException {
        }

        /** @deprecated */
        @Deprecated
        public void setup(com.aliyun.odps.mapred.TaskContext context) throws IOException {
                JoinPoint var3 = Factory.makeJP(ajc$tjp_0, this, this, context);
                OdpsDeprecatedLogger var10000 = OdpsDeprecatedLogger.aspectOf();
                Object[] var4 = new Object[]{this, context, var3};
                var10000.around((new MapperBase$AjcClosure1(var4)).linkClosureAndJoinPoint(69648));
        }

        /** @deprecated */
        @Deprecated
        public void map(long key, Record record, com.aliyun.odps.mapred.TaskContext context) throws IOException {
                StaticPart var10000 = ajc$tjp_1;
                Object[] var10 = new Object[]{Conversions.longObject(key), record, context};
                JoinPoint var9 = Factory.makeJP(var10000, this, this, var10);
                OdpsDeprecatedLogger var12 = OdpsDeprecatedLogger.aspectOf();
                Object[] var11 = new Object[]{this, Conversions.longObject(key), record, context, var9};
                var12.around((new MapperBase$AjcClosure3(var11)).linkClosureAndJoinPoint(69648));
        }

        /** @deprecated */
        @Deprecated
        public void cleanup(com.aliyun.odps.mapred.TaskContext context) throws IOException {
                JoinPoint var3 = Factory.makeJP(ajc$tjp_2, this, this, context);
                OdpsDeprecatedLogger var10000 = OdpsDeprecatedLogger.aspectOf();
                Object[] var4 = new Object[]{this, context, var3};
                var10000.around((new MapperBase$AjcClosure5(var4)).linkClosureAndJoinPoint(69648));
        }

        static {
                ajc$preClinit();
        }
}
