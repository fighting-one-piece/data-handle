package org.platform.modules.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
	a）把程序打包放到目标机器上去；
	b）进入hive客户端，添加jar包：
	hive>add jar /run/jar/udf_test.jar;
	c）创建临时函数：
	hive>CREATE TEMPORARY FUNCTION add_example AS 'hive.udf.Add';
	d）查询HQL语句：
	SELECT add_example(8, 9) FROM scores;
	SELECT add_example(scores.math, scores.art) FROM scores;
	SELECT add_example(6, 7, 8, 6.8) FROM scores;
	e）销毁临时函数：
	hive> DROP TEMPORARY FUNCTION add_example;
	细节在使用UDF的时候，会自动进行类型转换，
 *
 */
public class Add extends UDF {

	public Integer evaluate(Integer a, Integer b) {
		if (null == a || null == b) {
			return null;
		}
		return a + b;
	}

	public Double evaluate(Double a, Double b) {
		if (a == null || b == null)
			return null;
		return a + b;
	}

	public Integer evaluate(Integer... a) {
		int total = 0;
		for (int i = 0; i < a.length; i++)
			if (a[i] != null) total += a[i];
		return total;
	}
}
