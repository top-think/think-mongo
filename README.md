ThinkPHP 6.0 MongoDb驱动
===============

请使用最新版本的topthink/think-orm 本仓库不再维护更新

首先安装官方的mongodb扩展：

http://pecl.php.net/package/mongodb

然后，配置应用的数据库配置文件`database.php`的`type`参数改为：

~~~
'type'   =>  'Mongo',
~~~

即可正常使用MongoDb，例如：
~~~
Db::name('demo')
    ->find();
Db::name('demo')
    ->field('id,name')
    ->limit(10)
    ->order('id','desc')
    ->select();
~~~
