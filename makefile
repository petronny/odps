MAVEN=mvn
all:compile run

compile:
	$(MAVEN) clean install -DskipTests
run:
	$(MAVEN) exec:java -Dexec.mainClass=com.aliyun.odps.mapred.WordCount -Dexec.args="src.csv dest.csv" -Dexec.classpathScope=compile
zip:
	rm -f project.zip
	mkdir project
	cp -r lib src pom.xml project
	zip -rq project.zip project
	rm -rf project
