//package com.atguigu.wc;
//
//import org.apache.flink.api.common.typeinfo.TypeInfo;
//import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//
//import java.lang.reflect.Type;
//import java.util.Map;
//
//public class SSSFactory {
//
//}
//@TypeInfo(MyTupleTypeInfoFactory.class)
// class MyTuple<T0, T1> {
//    public T0 myfield0;
//    public T1 myfield1;
//}
//
//class MyTupleTypeInfoFactory extends TypeInfoFactory<MyTuple> {
//
//    @Override
//    public TypeInformation<MyTuple> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
//        return new MyTupleTypeInfoFactory(genericParameters.get("T0"), genericParameters.get("T1"));
//    }
//}