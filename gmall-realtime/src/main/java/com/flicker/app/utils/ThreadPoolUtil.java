package com.flicker.app.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {

    private ThreadPoolUtil(){};

    public static ThreadPoolExecutor getThreadPool(){
            return ThreadPoolExecutorFactory.threadPoolExecutor;
    }

    public static class ThreadPoolExecutorFactory{
        private static final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(8,
                16,
                1L,
                TimeUnit.MINUTES,
                new LinkedBlockingDeque<>());
    }
}
