TODO
========

Hi, 各位好。
经过我8小时的努力，终于结合ODPS的部分框架实现了这个题。
所用代码均是从原有代码中反汇编出来的。

下面我将对这些代码做出解释，并说明各位要做的改动做出说明

## 题目概述
就是要实现上个题目中的odps

现已将框架中需要自己实现的部分提取出来了，参见目录`src/main/java/com/aliyun/odps/mapred`

## Build & Run

需要安装好maven，请修改好makefile中的maven调用命令后

```
$ make compile
$ make run
```

## 代码概述
### WordCount
无需改动，相当于主程序。WordCount会调用JobRunnerImpl

### JobRunnerImpl
应该要实现一个可以子进程管理的东西。

现在的版本是直接调用LocalJobRunner的一个外壳，最好是把LocalJobRunner里面的东西都移进来，之后干掉LocalJobRunner

### LocalJobRunner
因为ODPS是在线的，所以有Local/Online两种调用接口，这个是Local的。它会:

- 调用WareHouse来获取数据
- 之后开若干小的MapDriver/ReduceDriver，来MR
- 输出接口调用(就是调用WareHouse)

需要去除没有调用的东西+各种肮脏手段。

### WareHouse
原来是从服务器拖数据下来，存到`warehouse/project_name/__tables__/tablename`下，里面有2个东西

- `__schema__`描述表结构用的，严格的说，如果文件太大分来存的话，文件列表也应该在里面。不过本题应该不用考虑了吧。
- data用来存放数据

我手动生成了这两个文件，并注释掉了源代码中的两句download。这里应该把这两处位置改成把那些csv表挪过来。`__schema__`的创建要根据WordCount中所写的创建。

不过本题中，输入就都是STRING了，输入中有几列就写几个STRING.需要各位手动实现。
并且需要去除没有调用的东西+各种肮脏手段。

### DriverBase

基类，去除没有调用的东西+各种肮脏手段。

### TaskContext

基类，去除没有调用的东西+各种肮脏手段。

### MapDriver

这个根据有没有Combiner类做了2个类似的类，不过本题中好像都是有Combiner的。
去除没有调用的东西+各种肮脏手段之后，
里面的run函数现在是再调用Mapper类实现功能，建议改成直接在这里实现。

### ReduceDriver

这个就只有一个类了，看起来清爽一点。
去除没有调用的东西+各种肮脏手段之后，
里面的run函数现在也是再调用Reducer类实现功能，建议改成直接在这里实现。

## 建议的分工

- LocalJobRunner & JobRunnerImpl
- WareHouse
- MapDriver & ReduceDriver & DriverBase & TaskContext

## 寻求帮助

请直接在[这里](https://github.com/petronny/odps/issues)发issue，注册完账号顺便follow我一下:)
