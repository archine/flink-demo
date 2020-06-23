package com.gjing.projects.flink.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author Gjing
 **/
public class DataSetSourceDemoJava {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
//        fromCollect(environment);
        fromFile(environment);
    }

    /**
     * 从集合开始读
     * @param environment ExecutionEnvironment
     * @throws Exception Exception
     */
    public static void fromCollect(ExecutionEnvironment environment) throws Exception {
        List<Integer> data = Arrays.asList(1, 2, 3, 4);
        environment.fromCollection(data).print();
    }

    /**
     * 从文件读
     * @param environment ExecutionEnvironment
     */
    public static void fromFile(ExecutionEnvironment environment) {
        try {
            environment.readTextFile("C:\\Users\\me\\Desktop\\flink\\test.txt").print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
