# Hive
源码;
 public static enum Mode {
    
    /**
     * PARTIAL1: from original data to partial aggregation data: iterate() and
     * terminatePartial() will be called.
     */
    PARTIAL1,
        /**
     * PARTIAL2: from partial aggregation data to partial aggregation data:
     * merge() and terminatePartial() will be called.
     */
    PARTIAL2,
        /**
     * FINAL: from partial aggregation to full aggregation: merge() and
     * terminate() will be called.
     */
    FINAL,
        /**
     * COMPLETE: from original data directly to full aggregation: iterate() and
     * terminate() will be called.
     */
    COMPLETE
  };
  
  
   /**
   * Initialize the evaluator.
   * 
   * @param m
   *          The mode of aggregation.
   * @param parameters
   *          The ObjectInspector for the parameters: In PARTIAL1 and COMPLETE
   *          mode, the parameters are original data; In PARTIAL2 and FINAL
   *          mode, the parameters are just partial aggregations (in that case,
   *          the array will always have a single element).
   * @return The ObjectInspector for the return value. In PARTIAL1 and PARTIAL2
   *         mode, the ObjectInspector for the return value of
   *         terminatePartial() call; In FINAL and COMPLETE mode, the
   *         ObjectInspector for the return value of terminate() call.
   * 
   *         NOTE: We need ObjectInspector[] (in addition to the TypeInfo[] in
   *         GenericUDAFResolver) for 2 reasons: 1. ObjectInspector contains
   *         more information than TypeInfo; and GenericUDAFEvaluator.init at
   *         execution time. 2. We call GenericUDAFResolver.getEvaluator at
   *         compilation time,
   */
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    // This function should be overriden in every sub class
    // And the sub class should call super.init(m, parameters) to get mode set.
    mode = m;
    return null;
  }
