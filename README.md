## 关于 dbcompare
dbcompare，即 db compare，是一个数据库之间的对比程序，原本是otter的一个子项目。 dbcompare 的作用是可以对数据库的数据进行并发对比，目前支持mysql。

## 使用
// doc need to be added

## build
需要下载 oracle 的 driver ： ojdbc6.jar。之后安装到本地 maven 仓库：
`mvn install:install-file -Dfile=ojdbc6.jar -DgroupId=com.oracle -DartifactId=ojdbc6 -Dversion=11.2.0.3 -Dpackaging=jar`

然后就可以使用 mavne 进行编译
