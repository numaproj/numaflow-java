//package io.numaproj.numaflow.function;
//
//import io.numaproj.numaflow.function.handlers.ReduceHandler;
//import io.numaproj.numaflow.function.handlers.ReducerFactory;
//import io.numaproj.numaflow.function.interfaces.Datum;
//import io.numaproj.numaflow.function.interfaces.Metadata;
//import io.numaproj.numaflow.function.types.Message;
//import io.numaproj.numaflow.function.types.MessageList;
//
//import java.util.Arrays;
//
//public class ReduceTestFactoryErr extends ReducerFactory<ReduceTestFactoryErr.ReduceTestFn> {
//    @Override
//    public ReduceTestFn createReducer() {
//        return new ReduceTestFn();
//    }
//
//    public static class ReduceTestFn extends ReduceHandler {
//        private int sum = 0;
//
//        @Override
//        public void addMessage(String[] keys, Datum datum, Metadata md) {
//            throw new RuntimeException("unknown exception");
//        }
//
//        @Override
//        public MessageList getOutput(String[] keys, Metadata md) {
//            String[] updatedKeys = Arrays
//                    .stream(keys)
//                    .map(c -> c + "-processed")
//                    .toArray(String[]::new);
//            return MessageList
//                    .newBuilder()
//                    .addMessage(new Message(String.valueOf(sum).getBytes(), updatedKeys))
//                    .build();
//        }
//    }
//}
