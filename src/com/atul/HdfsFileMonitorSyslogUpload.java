package com.atul;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URI;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.Event.CreateEvent;
import org.apache.hadoop.hdfs.inotify.Event.UnlinkEvent;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;

public class HdfsFileMonitorSyslogUpload {
	
	private static String SERVER = "164.99.175.165";
	
	private static PrintWriter writer = null;
	static{
		try{
			Socket soc = new Socket(SERVER,1468);
			writer = new PrintWriter(soc.getOutputStream());
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	static{
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run() {
				if(writer!=null){
					writer.close();
				}
			}
		});
	}

	public static void main(String[] args) throws IOException, InterruptedException, MissingEventsException {

		long lastReadTxid = 0;

		if (args.length > 1) {
			lastReadTxid = Long.parseLong(args[1]);
		}

		System.out.println("last Txid read = " + lastReadTxid);

		HdfsAdmin admin = new HdfsAdmin(URI.create(args[0]), new Configuration());

		DFSInotifyEventInputStream eventStream = admin.getInotifyEventStream(lastReadTxid);

		while (true) {
			EventBatch batch = eventStream.take();
			StringBuilder builder = new StringBuilder();
			builder.append("Current TxId = " + batch.getTxid());

			for (Event event : batch.getEvents()) {
				builder.append(" type = " + event.getEventType());
				switch (event.getEventType()) {
				case CREATE:
					CreateEvent createEvent = (CreateEvent) event;
					builder.append("  file = " + createEvent.getPath());
					builder.append("  owner = " + createEvent.getOwnerName());
					builder.append("  time = " + createEvent.getCtime());
					break;
				case UNLINK:
					UnlinkEvent unlinkEvent = (UnlinkEvent) event;
					builder.append("  file = " + unlinkEvent.getPath());
					builder.append("  time = " + unlinkEvent.getTimestamp());
					break;
				
				case APPEND:
				case CLOSE:
				case RENAME:
				default:
					break;
				}
			}
			forwardToSyslog(builder.toString());
		}
	}
	
	public static void forwardToSyslog(String str){
		System.out.println(str);
		
		try{			
			writer.write(header());
			writer.write(str);
			writer.write('\n');
			writer.flush();				
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static String header() throws Exception{
		StringBuilder builder = new StringBuilder();
		builder.append(InetAddress.getLocalHost().getHostName());
		builder.append(' ');
		builder.append(new Date().toString());
		builder.append(' ');
		return builder.toString();
	}
}

