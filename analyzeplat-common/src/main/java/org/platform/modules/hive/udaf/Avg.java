package org.platform.modules.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;

/**
用法:
1、一下两个包是必须的import org.apache.hadoop.hive.ql.exec.UDAF和 org.apache.hadoop.hive.ql.exec.UDAFEvaluator。
2、函数类需要继承UDAF类，内部类Evaluator实UDAFEvaluator接口。
3、Evaluator需要实现 init、iterate、terminatePartial、merge、terminate这几个函数。
a）init函数实现接口UDAFEvaluator的init函数。
b）iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean。
c）terminatePartial无参数，其为iterate函数轮转结束后，返回轮转数据，terminatePartial类似于hadoop的Combiner。
d）merge接收terminatePartial的返回结果，进行数据merge操作，其返回类型为boolean。
e）terminate返回最终的聚集函数结果。
执行步骤:
a）将java文件编译成Avg_test.jar。
b）进入hive客户端添加jar包：
hive>add jar /run/jar/Avg_test.jar。
c）创建临时函数：
hive>create temporary function avg_test 'hive.udaf.Avg';
d）查询语句：
hive>select avg_test(scores.math) from scores;
e）销毁临时函数：
hive>drop temporary function avg_test;
*/
public class Avg extends AbstractGenericUDAFResolver {
	
    public static class AvgState {
    	private long mCount;
    	private double mSum;
    }
    
    public static class AvgEvaluator implements UDAFEvaluator {
    	AvgState state;
    	public AvgEvaluator() {
              super();
              state = new AvgState();
              init();
    	}
    	
		/** * init函数类似于构造函数，用于UDAF的初始化 */
		public void init() {
		    state.mSum = 0;
		    state.mCount = 0;
		}
		
		/** * iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean * * @param o * @return */
		public boolean iterate(Double o) {
		    if (o != null) {
		    	state.mSum += o;
	            state.mCount++;
		    } return true;
		}
		
		/** 
		 * * terminatePartial无参数，其为iterate函数轮转结束后，返回轮转数据， 
		 * * terminatePartial类似于hadoop的Combiner 
		 * * @return 
		 * */
		public AvgState terminatePartial() {
		    // combiner
		    return state.mCount == 0 ? null : state;
		}
		
		/** 
		 * * merge接收terminatePartial的返回结果，进行数据merge操作，其返回类型为boolean 
		 * * @param o 
		 * * @return 
		 * */
		public boolean terminatePartial(AvgState o) {                
		    if (o != null) {
	            state.mCount += o.mCount;
	            state.mSum += o.mSum;
		    }
		    return true;
		}
		
		/** 
		 * * terminate返回最终的聚集函数结果 
		 * * @return 
		 * */
		public Double terminate() {
		    return state.mCount == 0 ? null : Double.valueOf(state.mSum / state.mCount);
		}
    }
}
