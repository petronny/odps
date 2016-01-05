MAVEN=mvn
all:compile run

compile:
	$(MAVEN) clean install -DskipTests
run:
	java -classpath target/classes:lib/odps-sdk-mapred-0.17.8-jar-with-dependencies.jar:lib/odps-mapred-local-0.15.0.jar:lib/odps-mapred-bridge-0.15.0.jar:lib/commons-collections-3.2.1.jar com.aliyun.odps.mapred.WordCount src.csv dest.csv
