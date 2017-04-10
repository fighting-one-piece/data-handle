package org.platform.utils;

import java.util.ArrayList;  
import java.util.List;  
  

import org.apache.thrift.TApplicationException;  
import org.apache.thrift.TException;  
import org.apache.thrift.TProcessor;  
import org.apache.thrift.protocol.TField;  
import org.apache.thrift.protocol.TJSONProtocol;  
import org.apache.thrift.protocol.TMessage;  
import org.apache.thrift.protocol.TMessageType;  
import org.apache.thrift.protocol.TProtocol;  
import org.apache.thrift.protocol.TProtocolFactory;  
import org.apache.thrift.protocol.TProtocolUtil;  
import org.apache.thrift.protocol.TStruct;  
import org.apache.thrift.protocol.TType;  
import org.apache.thrift.server.TServer;  
import org.apache.thrift.server.TThreadedSelectorServer;  
import org.apache.thrift.transport.TFastFramedTransport;  
import org.apache.thrift.transport.TNonblockingServerSocket;  
import org.apache.thrift.transport.TSocket;  
import org.apache.thrift.transport.TTransport;  
import org.apache.thrift.transport.TTransportFactory;

public class ThriftUtils {

	public static void main(String [] args) throws Exception {  
        //自定义的一个processor，非生成代码  
        ProxyProcessor proxyProcessor = new ProxyProcessor();     
        //指定的通信协议  
        TProtocolFactory tProtocolFactory = new TJSONProtocol.Factory();   
        //指定的通信方式  
        TTransportFactory tTransportFactory = new TFastFramedTransport.Factory();   
  
        int port = 4567;  
        TNonblockingServerSocket tNonblockingServerSocket =  
                new TNonblockingServerSocket(new TNonblockingServerSocket.NonblockingAbstractServerSocketArgs().port(port));  
        TThreadedSelectorServer.Args tThreadedSelectorServerArgs  
            = new TThreadedSelectorServer.Args(tNonblockingServerSocket);  
        tThreadedSelectorServerArgs.processor(proxyProcessor);  
        tThreadedSelectorServerArgs.protocolFactory(tProtocolFactory);  
        tThreadedSelectorServerArgs.transportFactory(tTransportFactory);  
        //指定的服务器模式    
        TServer serverEngine = new TThreadedSelectorServer(tThreadedSelectorServerArgs);  
        System.out.println("Starting the proxy on port " + port + "...");  
        serverEngine.serve();  
    }  
      
    static class ProxyProcessor implements TProcessor {  
        @Override  
        public boolean process(TProtocol in, TProtocol out) throws TException {  
            TMessage msg = in.readMessageBegin();  
            List<ProxyStruct> inDatas = readData(in);  
            in.readMessageEnd();  
              
            //转发请求到9090端口  
            TSocket socket = new TSocket("127.0.0.1", 9090);  
            socket.setTimeout(1000);  
            TTransport transport = socket;  
            transport = new TFastFramedTransport(transport);  
            TProtocol tProtocol = new TJSONProtocol(transport);  
            if ( !transport.isOpen() ) {  
                transport.open();  
            }  
              
            tProtocol.writeMessageBegin(msg);  
            wirteData(tProtocol, inDatas);   
            tProtocol.writeMessageEnd();  
            tProtocol.getTransport().flush();  
              
            int seqid = msg.seqid;  
            String methodName = msg.name;  
            msg = tProtocol.readMessageBegin();  
            if (msg.type == TMessageType.EXCEPTION) {  
                TApplicationException x = TApplicationException.read(tProtocol);  
                tProtocol.readMessageEnd();  
                throw x;  
            }  
            if (msg.seqid != seqid) {  
                throw new TApplicationException(TApplicationException.BAD_SEQUENCE_ID, methodName + " failed: out of sequence response");  
            }  
            inDatas = readData(tProtocol);   
            tProtocol.readMessageEnd();  
              
            out.writeMessageBegin(msg);  
            wirteData(out, inDatas);   
            out.writeMessageEnd();  
            out.getTransport().flush();  
              
            return true;  
        }  
          
        @SuppressWarnings("unchecked")
		private void wirteData(TProtocol out, List<ProxyStruct> outDatas) throws TException {  
            out.writeStructBegin(new TStruct(""));  
            for (ProxyStruct outData : outDatas) {  
                TField field = outData.field;  
                Object value = outData.value;  
                out.writeFieldBegin(field);  
                  
                switch (field.type) {  
                    case TType.VOID:  
                        break;  
                    case TType.BOOL:  
                        out.writeBool((boolean)value);  
                        break;  
                    case TType.BYTE:  
                        out.writeByte((byte)value);  
                        break;  
                    case TType.DOUBLE:  
                        out.writeDouble((double)value);  
                        break;  
                    case TType.I16:  
                        out.writeI16((short)value);  
                        break;  
                    case TType.I32:  
                        out.writeI32((int)value);  
                        break;  
                    case TType.I64:  
                        out.writeI64((long)value);  
                        break;  
                    case TType.STRING:  
                        out.writeString((String)value);  
                        break;  
                    case TType.STRUCT:  
                        wirteData(out, (List<ProxyStruct>)value);  
                        break;  
                    case TType.MAP:  
                        //out.writeMapBegin((TMap)value);  
                        //out.writeMapEnd();  
                        break;  
                    case TType.SET:  
                        //out.writeSetBegin((TSet)value);  
                        //out.writeSetEnd();  
                        break;  
                    case TType.LIST:  
                        //out.writeListBegin((TList)value);  
                        //out.writeListEnd();  
                        break;  
                    case TType.ENUM:  
                        break;  
                    default:  
                }  
                  
                out.writeFieldEnd();  
            }  
            out.writeFieldStop();  
            out.writeStructEnd();  
        }  
          
        private List<ProxyStruct> readData(TProtocol in) throws TException {  
            List<ProxyStruct> inDatas = new ArrayList<ProxyStruct>();  
            TField schemeField;  
            in.readStructBegin();  
            while (true) {  
                schemeField = in.readFieldBegin();  
                if (schemeField.type == TType.STOP) {   
                    break;  
                }  
                ProxyStruct inData = null;  
                  
                switch (schemeField.type) {  
                    case TType.VOID:  
                        TProtocolUtil.skip(in, schemeField.type);  
                        break;  
                    case TType.BOOL:  
                        inData = new ProxyStruct(schemeField, in.readBool());  
                        break;  
                    case TType.BYTE:  
                        inData = new ProxyStruct(schemeField, in.readByte());  
                        break;  
                    case TType.DOUBLE:  
                        inData = new ProxyStruct(schemeField, in.readDouble());  
                        break;  
                    case TType.I16:  
                        inData = new ProxyStruct(schemeField, in.readI16());  
                        break;  
                    case TType.I32:  
                        inData = new ProxyStruct(schemeField, in.readI32());  
                        System.out.println("I32-->" + inData.value);   
                        break;  
                    case TType.I64:  
                        inData = new ProxyStruct(schemeField, in.readI64());  
                        break;  
                    case TType.STRING:  
                        inData = new ProxyStruct(schemeField, in.readString());  
                        System.out.println("STRING-->" + inData.value);  
                        break;  
                    case TType.STRUCT:  
                        inData = new ProxyStruct(schemeField, readData(in));  
                        break;  
                    case TType.MAP:  
                        //inData = new ProxyStruct(schemeField, in.readMapBegin());  
                        /** 
                         * 这里我懒了，不想写了，readMapBegin返回的TMap对象有3个字段 
                         * keyType，valueType，size，没错就是map的key的类型，value的类型，map的大小 
                         * 从0到size累计循环的按类型读取key和读取value，构造一个hashmap就可以了 
                         */  
                        //in.readMapEnd();  
                        break;  
                    case TType.SET:  
                        //inData = new ProxyStruct(schemeField, in.readSetBegin());  
                        //同理MAP类型  
                        //in.readSetEnd();  
                        break;  
                    case TType.LIST:  
                        //inData = new ProxyStruct(schemeField, in.readListBegin());  
                        //同理MAP类型  
                        //in.readListEnd();  
                        break;  
                    case TType.ENUM:  
                        //Enum类型传输时是个i32  
                        TProtocolUtil.skip(in, schemeField.type);  
                        break;  
                    default:  
                        TProtocolUtil.skip(in, schemeField.type);  
                }  
                if (inData != null ) inDatas.add(inData);  
                  
                in.readFieldEnd();  
            }  
            in.readStructEnd();  
              
            return inDatas;  
        }  
          
    }  
      
    //用来存储读取到的各种类型的字段  
    static class ProxyStruct {  
        public TField field;  
        public Object value;  
          
        public ProxyStruct(TField tField, Object object) {  
            field = tField;  
            value = object;  
        }  
    }  
	
}
