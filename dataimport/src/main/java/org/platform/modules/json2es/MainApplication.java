package org.platform.modules.json2es;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

import org.platform.utils.file.FileOpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainApplication {
	
	private static Logger LOG = LoggerFactory.getLogger(MainApplication.class);
	
	private static ExecutorService threadPool = new ThreadPoolExecutor(2, 5, 10, TimeUnit.SECONDS, 
		new LinkedBlockingDeque<Runnable>(50), Executors.defaultThreadFactory(), new CallerRunsPolicy());
	
	/**
	 * 1、并发数
	 * 2、输入路径
	 * 3、ES INDEX
	 * 4、ES TYPE
	 * 5、BATCH SIZE 批量大小
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 5) {
			System.err.println("params error! please check!");
		}
		int permits = Integer.parseInt(args[0]);
		Semaphore semaphore = new Semaphore(permits);
		List<String> filePaths = FileOpUtils.readAllFiles(args[1]);
		for (int i = 0, len = filePaths.size(); i < len; i++) {
			try {
				semaphore.acquire();
			} catch (InterruptedException e) {
				LOG.error(e.getMessage(), e);
			}
			System.out.println(filePaths.get(i) + " acquire permit !");
			threadPool.submit(new Task(filePaths.get(i), args[2], 
				args[3], Integer.parseInt(args[4]), semaphore));
		}
		while (semaphore.availablePermits() != permits) {}
		System.exit(0);
	}

}


