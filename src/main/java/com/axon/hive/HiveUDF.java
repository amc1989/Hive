package com.axon.hive;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 根据业务需求，重载不同参数类型和个数的evaluate(String str)方法即可
 * @author zhulei
 *
 */
@Description(name="uptocase",value="transfer dowm to upcase")
public class HiveUDF extends UDF{
    
	 public String evaluate(String str){
		return str.toUpperCase();
		 
	 }
                                                                                 
}
