package com.axon.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
/**
 * 
 * @author zhulei
 * 这个抽象类与class UDF的区别就是，GenericUDF可以一些集合类型的参数比如：List，Map，Array，Set
 */

public class HiveGenericUDF extends GenericUDF {
	/**
	 * 用于判断自定义函数传进来参数的个数是否正确，参数类型是否正确，以及确定这个自定义函数最后的返回值的类型
	 */
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		if (arguments.length != 2) {
			throw new UDFArgumentLengthException(
					"there are  only  2 arguments in this array");
		}
		ObjectInspector s1 = arguments[0];
		ObjectInspector s2 = arguments[0];
		if(!(s1 instanceof ObjectInspector)||!(s2 instanceof ObjectInspector)){
			throw new UDFArgumentException("both of arguments are in same type");
		}
		//你要返回的结果的类型
		return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}
	/**
	 * 该函数就是hive的自定义函数的具体的业务逻辑的处理函数，我这个函数的功能就是输入为两个值，返回第一个不为NULL的那个值
	 */
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		if(null==arguments[0].get())
			return (String) arguments[1].get().toString();
		return arguments[0].get().toString();
	}
	/**
	 * 这个就是exlpain这个udf的说明
	 */
	@Override
	public String getDisplayString(String[] children) {
		
		return "int jiajia";
	}
}
