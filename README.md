# MIT6824 spring 2023
博客地址:https://rioblog.fun/blog/mit6824/

debug用了https://blog.josejg.com/debugging-pretty/这个方案，在多线程编程中能更直观地看出各个节点之间的交互

```
VERBOSE=1 go test -run Backup | ../../pretty-log -c 5 -i TIMR,DROP,LOG2
利用这个指令可以以3列展示输出
```
```
 ../../dstest -p 10 -n 100 -v 1 -o ../../output3.log TestSnapshotBasic2D
这个指令可以以 10 个线程并行地执行TestSnapshotBasic2D 100 次，输出到output3.log文件夹
 ```
 