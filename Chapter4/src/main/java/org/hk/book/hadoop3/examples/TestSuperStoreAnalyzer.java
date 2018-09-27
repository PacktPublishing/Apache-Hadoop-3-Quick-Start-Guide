package org.hk.book.hadoop3.examples;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.*;
import static org.mockito.Mockito.*;

public class TestSuperStoreAnalyzer {
	
	@Test
	public void testSuperStoreMapper() throws Exception {
		SuperStoreAnalyzer.SuperStoreMapper sm = 
				new SuperStoreAnalyzer.SuperStoreMapper();
		LongWritable lw = new LongWritable();
		lw.set(1232123123);
		Text tt = new Text("Item1");
		
		Mapper.Context mockContext = mock(Mapper.Context.class); 
		sm.map(lw, tt, mockContext);
	}
}
