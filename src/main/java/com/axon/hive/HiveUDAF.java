package com.axon.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;

/**
 * 
 * @author zhulei
 *
 */
public class HiveUDAF extends GenericUDAFEvaluator {
	private PrimitiveObjectInspector primitiveObjectInspector;
	private StandardListObjectInspector standardListObjectInspector;

	/**
	 * 该函数还是对这个udaf最后结果输出的类型进行确定，省去了对参数个数的判断
	 * 
	 * 根据m传进来的值为：PARTIAL1和COMPLETE则为基本类型的数据数据（源码有解释），直接转换成list类型返回即可
	 * 如果不是这个类型，那么就是其他的数据类型
	 * 
	 * 参数ObjectInspector[] parameters 只有一个值，在源码中已经标出了。
	 */
	@Override
	public ObjectInspector init(Mode m, ObjectInspector[] parameters)
			throws HiveException {
		super.init(m, parameters);
		if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
			primitiveObjectInspector = (PrimitiveObjectInspector) parameters[0];
			return ObjectInspectorFactory
					.getStandardListObjectInspector(ObjectInspectorUtils
							.getStandardObjectInspector(primitiveObjectInspector));
		} else {
			if (!(parameters[0] instanceof StandardListObjectInspector)) {

				primitiveObjectInspector = (PrimitiveObjectInspector) ObjectInspectorUtils
						.getStandardObjectInspector(parameters[0]);
				return ObjectInspectorFactory
						.getStandardListObjectInspector(primitiveObjectInspector);
			} else {
				standardListObjectInspector = (StandardListObjectInspector) parameters[0];
				primitiveObjectInspector = (PrimitiveObjectInspector) standardListObjectInspector
						.getListElementObjectInspector();
				return ObjectInspectorFactory
						.getStandardListObjectInspector(ObjectInspectorUtils
								.getStandardObjectInspector(standardListObjectInspector));
			}
		}

	}

	/**
	 * 构造个内部类，用于存储数据
	 */
	@SuppressWarnings("deprecation")
	static class MyListBuffered implements AggregationBuffer {
		List<Object> list;
	}

	/**
	 * 获取一个AggregationBuffer的对象. getNewAggregationBuffer方法和reset方法是一起使用的
	 */
	@SuppressWarnings("deprecation")
	@Override
	public AggregationBuffer getNewAggregationBuffer() throws HiveException {
		MyListBuffered myListBuffered = new MyListBuffered();
		//
		reset(myListBuffered);
		return myListBuffered;
	}

	/**
	 * 重新设置聚合对象
	 * 
	 */
	@SuppressWarnings("deprecation")
	@Override
	public void reset(AggregationBuffer agg) throws HiveException {
		MyListBuffered myListBuffered = (MyListBuffered) agg;
		myListBuffered.list = new ArrayList<Object>();
	}

	/**
	 * 对原始数据进行迭代.
	 * 
	 * @param parameters
	 *            需要迭代的对象，不过要要对这个对象进行类型转换，详细看putIntoList方法
	 */
	@SuppressWarnings("deprecation")
	@Override
	public void iterate(AggregationBuffer agg, Object[] parameters)
			throws HiveException {
		assert (parameters.length == 1);
		Object object = parameters[0];
		if (object != null) {
			MyListBuffered myListBuffered = (MyListBuffered) agg;
			putIntoList(object, myListBuffered);
		}

	}

	/**
	 * 
	 * @param object
	 *            不知道具体的数据类型，通过ObjectInspectorUtils.copyToStandardObject转换成具体的
	 *            基本数据类型
	 * @param myListBuffered
	 *            对每行的数据进行聚合
	 */
	private void putIntoList(Object object, MyListBuffered myListBuffered) {
		Object object2 = ObjectInspectorUtils.copyToStandardObject(object,
				primitiveObjectInspector);
		myListBuffered.list.add(object2);
	}

	/**
	 * 
	 * 就是对部分数据的存储对象进行转换。 这个结果类型必须是我们在init方法中指出的最后的结果的输出类型
	 * 
	 * @return partial aggregation result.
	 */
	@SuppressWarnings("deprecation")
	@Override
	public Object terminatePartial(AggregationBuffer agg) throws HiveException {
		MyListBuffered myListBuffered = (MyListBuffered) agg;
		List<Object> list = new ArrayList<Object>(myListBuffered.list.size());
		list.addAll(myListBuffered.list);
		return list;
	}
	
	
	/**
	 * @param agg   就是聚合数据的对象
	 * 
	 * @param partial ：是我们最终结果要返回的类型，因此必须先转换我们要返回的类型，比如：list，那么这个对象引用
	 * 就包含了所有的对象，需要对其把list里面的对象，添加到 agg这个聚合数据的对象中
	 * 
	 */
	@SuppressWarnings({ "deprecation", "unchecked" })
	@Override
	public void merge(AggregationBuffer agg, Object partial)
			throws HiveException {
		MyListBuffered myListBuffered = (MyListBuffered) agg;
		ArrayList<Object> arrayList = (ArrayList<Object>) standardListObjectInspector
				.getList(partial);
           for(Object object:arrayList ){
        	   putIntoList(object,myListBuffered);
           }
	}
   
	/**
     * 对所有的数据进行整合
     */
	@SuppressWarnings("deprecation")
	@Override
	public Object terminate( AggregationBuffer agg) throws HiveException {
		MyListBuffered myListBuffered = (MyListBuffered) agg;
		ArrayList<Object> arrayList = new ArrayList<Object>(myListBuffered.list.size());
		return arrayList.addAll(myListBuffered.list);
	}

}
