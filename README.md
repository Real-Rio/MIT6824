# MIT6824 spring 2023
博客地址:https://rioblog.fun/blog/mit6824/

debug用了https://blog.josejg.com/debugging-pretty/这个方案，在多线程编程中能更直观地看出各个节点之间的交互

```
VERBOSE=1 go test -run Backup | dslogs -c 5 -i TIMR,DROP,LOG2```
利用这个指令可以以3列展示输出