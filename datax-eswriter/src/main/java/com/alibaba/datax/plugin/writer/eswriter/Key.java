package com.alibaba.datax.plugin.writer.eswriter;

public final class Key {
	
	/*
	 * @name:  esClusterName
	 * @description:  elastic search cluster name
	*/
	public final static String esClusterName = "esClusterName";
	
	/*
	 * @name:  esClusterIP
	 * @description:  elastic search cluster ip
	*/
	public final static String esClusterIP = "esClusterIP";
	
	/*
	 * @name:  esClusterPort
	 * @description:  elastic search cluster port
	*/
	public final static String esClusterPort = "esClusterPort";
	
	/*
	 * @name: esIndex 
	 * @description:  elastic search index
	 */
	public final static String esIndex = "esIndex";
	
	/*
	 * @name: esType
	 * @description:  elastic search type
	 */
	public final static String esType = "esType";
	
	/*
	 * @name: attributeNameString
	 * @description:  attribute name list 
	 */
	public final static String attributeNameString = "attributeNameString";
	
	/*
	 * @name: attributeNameSplit
	 * @description: separator to split attribute name string
	 * @range:
	 * @mandatory: false
	 * @default:\t
	 */
	public final static String attributeNameSplit = "attributeNameSplit";
	
	/*
	 * @name: className
	 * @description: qualified class name 
	 */
	public final static String className = "className";
	
	/*
	 * @name: batchSize
	 * @description: commit to elasticsearch batch size
	 */
	public final static String batchSize = "batchSize";
	
}

