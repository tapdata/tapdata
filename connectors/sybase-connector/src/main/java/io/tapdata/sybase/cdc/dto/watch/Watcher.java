package io.tapdata.sybase.cdc.dto.watch;

import com.sun.nio.file.SensitivityWatchEventModifier;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

/**
 * @author GavinXiao
 * @description Watcher create by Gavin
 * @create 2023/7/12 18:48
 *
 * https://blog.csdn.net/lemon_TT/article/details/126063377
 **/
public class Watcher {


    public static void main(String[] args) throws IOException {
        // 这里的监听必须是目录
        Path path = Paths.get("static");
        // 创建WatchService，它是对操作系统的文件监视器的封装，相对之前，不需要遍历文件目录，效率要高很多
        WatchService watcher = FileSystems.getDefault().newWatchService();
        // 注册指定目录使用的监听器，监视目录下文件的变化；
        // PS：Path必须是目录，不能是文件；
        // StandardWatchEventKinds.ENTRY_MODIFY，表示监视文件的修改事件
        path.register(watcher, new WatchEvent.Kind[]{StandardWatchEventKinds.ENTRY_MODIFY},
                SensitivityWatchEventModifier.LOW);

        // 创建一个线程，等待目录下的文件发生变化
        try {
            while (true) {
                // 获取目录的变化:
                // take()是一个阻塞方法，会等待监视器发出的信号才返回。
                // 还可以使用watcher.poll()方法，非阻塞方法，会立即返回当时监视器中是否有信号。
                // 返回结果WatchKey，是一个单例对象，与前面的register方法返回的实例是同一个；
                WatchKey key = watcher.take();
                // 处理文件变化事件：
                // key.pollEvents()用于获取文件变化事件，只能获取一次，不能重复获取，类似队列的形式。
                for (WatchEvent<?> event : key.pollEvents()) {
                    // event.kind()：事件类型
                    if (event.kind() == StandardWatchEventKinds.OVERFLOW) {
                        //事件可能lost or discarded
                        continue;
                    }
                    // 返回触发事件的文件或目录的路径（相对路径）
                    Path fileName = (Path) event.context();
                    System.out.println("文件更新: " + fileName);
                }
                // 每次调用WatchService的take()或poll()方法时需要通过本方法重置
                if (!key.reset()) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
