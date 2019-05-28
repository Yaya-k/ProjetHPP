package projectQuery1;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

import entities.PostCom;
import traitement.FileReader;
import traitement.Processor;
import traitement.QueuesSorter;

public class MainTest {

	@Test
	public void testFileReader () throws InterruptedException {
		
		String test = "Q1BigTest";
		
		BlockingQueue<PostCom> blockingQueuePost = new ArrayBlockingQueue<PostCom>(50);
		BlockingQueue<PostCom> blockingQueueComment = new ArrayBlockingQueue<PostCom>(50);
		
		File fPost = new File("Tests\\" + test + "\\posts.dat");
		File fComment = new File("Tests\\" + test + "\\comments.dat");
		
		String sExpectedPost = "[\n" + 
				"P: 2010-02-09 04:05:10.421 | 529360, \n" + 
				"P: 2010-02-09 16:21:23.008 | 1076792, \n" + 
				"P: 2010-02-11 12:01:34.646 | 1301393, \n" + 
				"P: 2010-02-11 19:23:25.459 | 299390, \n" + 
				"P: 2010-02-11 19:23:26.459 | 299391, \n" + 
				"P: 2010-02-11 19:23:27.459 | 299392, \n" + 
				"P: 2010-02-11 19:23:28.459 | 299393, \n" + 
				"P: 2010-02-11 19:23:29.459 | 299394, \n" + 
				"P: 2010-02-11 19:23:30.459 | 299395, \n" + 
				"P: 2010-02-12 10:37:40.873 | 798675, \n" + 
				"P: 2010-02-12 18:19:04.855 | 701070, \n" + 
				"P: 2010-02-13 01:54:47.702 | 705054, \n" + 
				"P: 2010-02-16 06:18:31.926 | 571477, \n" + 
				"null | -1]";
		String sExpectedComment = "[\n" + 
				"C: 2010-02-09 04:05:20.777 | 529590, \n" + 
				"C: 2010-02-09 04:20:53.281 | 529589, \n" + 
				"C: 2010-02-09 05:19:19.802 | 529591, \n" + 
				"C: 2010-02-09 06:08:38.206 | 529594, \n" + 
				"C: 2010-02-09 06:31:37.964 | 529592, \n" + 
				"C: 2010-02-09 07:19:44.946 | 529595, \n" + 
				"C: 2010-02-09 08:17:12.246 | 529588, \n" + 
				"C: 2010-02-12 18:23:49.21 | 702760, \n" + 
				"C: 2010-02-12 18:38:09.433 | 702747, \n" + 
				"C: 2010-02-12 18:50:35.967 | 702757, \n" + 
				"C: 2010-02-12 20:37:02.578 | 702759, \n" + 
				"C: 2010-02-12 22:05:25.252 | 702755, \n" + 
				"C: 2010-02-13 01:11:10.978 | 702752, \n" + 
				"C: 2010-02-13 07:15:06.967 | 702754, \n" + 
				"C: 2010-02-13 08:12:06.616 | 702750, \n" + 
				"C: 2010-02-13 09:58:17.093 | 702753, \n" + 
				"C: 2010-02-13 10:43:48.77 | 702756, \n" + 
				"C: 2010-02-16 06:51:56.345 | 571692, \n" + 
				"null | -1]";
		
		ExecutorService service = Executors.newFixedThreadPool(2);
		
		FileReader pReader;
		Thread pReaderThread = null;
		try {
			pReader = new FileReader(blockingQueuePost, fPost, true);
			pReaderThread = new Thread(pReader);
			service.execute(pReaderThread);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		FileReader cReader;
		Thread cReaderThread = null;
		try {
			cReader = new FileReader(blockingQueueComment, fComment, false);
			cReaderThread = new Thread(cReader);
			service.execute(cReaderThread);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		Main.shutdownAndAwaitTermination(service);
		assertEquals(sExpectedPost, blockingQueuePost.toString());
		assertEquals(sExpectedComment, blockingQueueComment.toString());
	}

	@Test
	public void testQueuesSorter () throws InterruptedException {
		
		String test = "Q1BigTest";		
		
		BlockingQueue<PostCom> blockingQueuePost = new ArrayBlockingQueue<PostCom>(50);
		BlockingQueue<PostCom> blockingQueueComment = new ArrayBlockingQueue<PostCom>(50);
		BlockingQueue<PostCom> blockingQueuePostCom = new ArrayBlockingQueue<PostCom>(100);
		
		File fPost = new File("Tests\\" + test + "\\posts.dat");
		File fComment = new File("Tests\\" + test + "\\comments.dat");
		
		String sExpectedSortedQueue = "[\n" + 
				"P: 2010-02-09 04:05:10.421 | 529360, \n" + 
				"C: 2010-02-09 04:05:20.777 | 529590, \n" + 
				"C: 2010-02-09 04:20:53.281 | 529589, \n" + 
				"C: 2010-02-09 05:19:19.802 | 529591, \n" + 
				"C: 2010-02-09 06:08:38.206 | 529594, \n" + 
				"C: 2010-02-09 06:31:37.964 | 529592, \n" + 
				"C: 2010-02-09 07:19:44.946 | 529595, \n" + 
				"C: 2010-02-09 08:17:12.246 | 529588, \n" + 
				"P: 2010-02-09 16:21:23.008 | 1076792, \n" + 
				"P: 2010-02-11 12:01:34.646 | 1301393, \n" + 
				"P: 2010-02-11 19:23:25.459 | 299390, \n" + 
				"P: 2010-02-11 19:23:26.459 | 299391, \n" + 
				"P: 2010-02-11 19:23:27.459 | 299392, \n" + 
				"P: 2010-02-11 19:23:28.459 | 299393, \n" + 
				"P: 2010-02-11 19:23:29.459 | 299394, \n" + 
				"P: 2010-02-11 19:23:30.459 | 299395, \n" + 
				"P: 2010-02-12 10:37:40.873 | 798675, \n" + 
				"P: 2010-02-12 18:19:04.855 | 701070, \n" + 
				"C: 2010-02-12 18:23:49.21 | 702760, \n" + 
				"C: 2010-02-12 18:38:09.433 | 702747, \n" + 
				"C: 2010-02-12 18:50:35.967 | 702757, \n" + 
				"C: 2010-02-12 20:37:02.578 | 702759, \n" + 
				"C: 2010-02-12 22:05:25.252 | 702755, \n" + 
				"C: 2010-02-13 01:11:10.978 | 702752, \n" + 
				"P: 2010-02-13 01:54:47.702 | 705054, \n" + 
				"C: 2010-02-13 07:15:06.967 | 702754, \n" + 
				"C: 2010-02-13 08:12:06.616 | 702750, \n" + 
				"C: 2010-02-13 09:58:17.093 | 702753, \n" + 
				"C: 2010-02-13 10:43:48.77 | 702756, \n" + 
				"P: 2010-02-16 06:18:31.926 | 571477, \n" + 
				"C: 2010-02-16 06:51:56.345 | 571692, \n" +
				"null | -1]";
		
		ExecutorService service = Executors.newFixedThreadPool(3);
		
		FileReader pReader;
		Thread pReaderThread = null;
		try {
			pReader = new FileReader(blockingQueuePost, fPost, true);
			pReaderThread = new Thread(pReader);
			service.execute(pReaderThread);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		FileReader cReader;
		Thread cReaderThread = null;
		try {
			cReader = new FileReader(blockingQueueComment, fComment, false);
			cReaderThread = new Thread(cReader);
			service.execute(cReaderThread);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		QueuesSorter sorter = new QueuesSorter(blockingQueuePost, blockingQueueComment, blockingQueuePostCom);
		Thread sorterThread = new Thread(sorter);
		service.execute(sorterThread);
		
		Main.shutdownAndAwaitTermination(service);
		assertEquals(sExpectedSortedQueue, blockingQueuePostCom.toString());
	}

	@Test
	public void testQ1Basic() {
		String expected = "2010-02-01 05:12:32.921 | 1039993, Lei Liu, 10, 0 | -, -, -, - | -, -, -, -\n" + 
				"2010-02-02 19:53:43.226 | 299101, Michael Wang, 10, 0 | 1039993, Lei Liu, 9, 0 | -, -, -, -\n" + 
				"2010-02-09 04:05:10.421 | 529360, Wei Zhu, 10, 0 | 299101, Michael Wang, 4, 0 | 1039993, Lei Liu, 3, 0\n";
		String result = testRun("Q1Basic");
		assertEquals(expected, result);
	}
	
	@Test
	public void testQ1Basic2() {
		String expected = "2010-02-01 05:12:32.921 | 1039993, Lei Liu, 10, 0 | -, -, -, - | -, -, -, -\n" + 
				"2010-02-02 19:53:43.226 | 299101, Michael Wang, 10, 0 | 1039993, Lei Liu, 9, 0 | -, -, -, -\n" + 
				"2010-02-09 04:05:10.421 | 529360, Wei Zhu, 10, 0 | 299101, Michael Wang, 4, 0 | 1039993, Lei Liu, 3, 0\n" + 
				"2010-02-10 04:05:20.777 | 1039993, Lei Liu, 12, 1 | 529360, Wei Zhu, 9, 0 | 299101, Michael Wang, 3, 0\n";
		String result = testRun("Q1Basic2");
		assertEquals(expected, result);
	}
	
	@Test
	public void testQ1BigTest() {
		String expected = "2010-02-09 04:05:10.421 | 529360, Wei Zhu, 10, 0 | -, -, -, - | -, -, -, -\n" + 
				"2010-02-09 04:05:20.777 | 529360, Wei Zhu, 20, 1 | -, -, -, - | -, -, -, -\n" + 
				"2010-02-09 04:20:53.281 | 529360, Wei Zhu, 30, 2 | -, -, -, - | -, -, -, -\n" + 
				"2010-02-09 05:19:19.802 | 529360, Wei Zhu, 40, 3 | -, -, -, - | -, -, -, -\n" + 
				"2010-02-09 06:08:38.206 | 529360, Wei Zhu, 50, 4 | -, -, -, - | -, -, -, -\n" + 
				"2010-02-09 06:31:37.964 | 529360, Wei Zhu, 60, 5 | -, -, -, - | -, -, -, -\n" + 
				"2010-02-09 07:19:44.946 | 529360, Wei Zhu, 70, 6 | -, -, -, - | -, -, -, -\n" + 
				"2010-02-09 08:17:12.246 | 529360, Wei Zhu, 80, 7 | -, -, -, - | -, -, -, -\n" + 
				"2010-02-09 16:21:23.008 | 529360, Wei Zhu, 80, 7 | 1076792, Richard Richter, 10, 0 | -, -, -, -\n" + 
				"2010-02-11 12:01:34.646 | 529360, Wei Zhu, 64, 7 | 1301393, Wei Zhou, 10, 0 | 1076792, Richard Richter, 9, 0\n" + 
				"2010-02-11 19:23:25.459 | 529360, Wei Zhu, 64, 7 | 299390, Michael Wang, 10, 0 | 1301393, Wei Zhou, 10, 0\n" + 
				"2010-02-11 19:23:26.459 | 529360, Wei Zhu, 64, 7 | 299391, Michael Wang, 10, 0 | 299390, Michael Wang, 10, 0\n" + 
				"2010-02-11 19:23:27.459 | 529360, Wei Zhu, 64, 7 | 299392, Michael Wang, 10, 0 | 299391, Michael Wang, 10, 0\n" + 
				"2010-02-11 19:23:28.459 | 529360, Wei Zhu, 64, 7 | 299393, Michael Wang, 10, 0 | 299392, Michael Wang, 10, 0\n" + 
				"2010-02-11 19:23:29.459 | 529360, Wei Zhu, 64, 7 | 299394, Michael Wang, 10, 0 | 299393, Michael Wang, 10, 0\n" + 
				"2010-02-11 19:23:30.459 | 529360, Wei Zhu, 64, 7 | 299395, Michael Wang, 10, 0 | 299394, Michael Wang, 10, 0\n" + 
				"2010-02-12 10:37:40.873 | 529360, Wei Zhu, 56, 7 | 798675, Peter Schmidt, 10, 0 | 299395, Michael Wang, 10, 0\n" + 
				"2010-02-12 18:19:04.855 | 529360, Wei Zhu, 56, 7 | 701070, Emperor of Brazil Silva, 10, 0 | 798675, Peter Schmidt, 10, 0\n" + 
				"2010-02-12 18:23:49.21 | 529360, Wei Zhu, 56, 7 | 701070, Emperor of Brazil Silva, 20, 1 | 798675, Peter Schmidt, 10, 0\n" + 
				"2010-02-12 18:38:09.433 | 529360, Wei Zhu, 56, 7 | 701070, Emperor of Brazil Silva, 30, 2 | 798675, Peter Schmidt, 10, 0\n" + 
				"2010-02-12 18:50:35.967 | 529360, Wei Zhu, 56, 7 | 701070, Emperor of Brazil Silva, 40, 3 | 798675, Peter Schmidt, 10, 0\n" + 
				"2010-02-12 20:37:02.578 | 529360, Wei Zhu, 56, 7 | 701070, Emperor of Brazil Silva, 50, 4 | 798675, Peter Schmidt, 10, 0\n" + 
				"2010-02-12 22:05:25.252 | 701070, Emperor of Brazil Silva, 60, 5 | 529360, Wei Zhu, 56, 7 | 798675, Peter Schmidt, 10, 0\n" + 
				"2010-02-13 01:11:10.978 | 701070, Emperor of Brazil Silva, 70, 6 | 529360, Wei Zhu, 56, 7 | 798675, Peter Schmidt, 10, 0\n" + 
				"2010-02-13 01:54:47.702 | 701070, Emperor of Brazil Silva, 70, 6 | 529360, Wei Zhu, 56, 7 | 705054, Jun Lu, 10, 0\n" + 
				"2010-02-13 07:15:06.967 | 701070, Emperor of Brazil Silva, 80, 7 | 529360, Wei Zhu, 50, 7 | 705054, Jun Lu, 10, 0\n" + 
				"2010-02-13 08:12:06.616 | 701070, Emperor of Brazil Silva, 90, 8 | 529360, Wei Zhu, 49, 7 | 705054, Jun Lu, 10, 0\n" + 
				"2010-02-13 09:58:17.093 | 701070, Emperor of Brazil Silva, 100, 9 | 529360, Wei Zhu, 48, 7 | 705054, Jun Lu, 10, 0\n" + 
				"2010-02-13 10:43:48.77 | 701070, Emperor of Brazil Silva, 110, 10 | 529360, Wei Zhu, 48, 7 | 705054, Jun Lu, 10, 0\n" + 
				"2010-02-16 06:18:31.926 | 701070, Emperor of Brazil Silva, 81, 10 | 529360, Wei Zhu, 27, 7 | 571477, Jharana Bajracharya Rashid Shrestha, 10, 0\n" + 
				"2010-02-16 06:51:56.345 | 701070, Emperor of Brazil Silva, 81, 10 | 529360, Wei Zhu, 26, 7 | 571477, Jharana Bajracharya Rashid Shrestha, 20, 1\n";
		String result = testRun("Q1BigTest");
		assertEquals(expected, result);
	}
	
	@Test
	public void testQ1Case1() {
		String expected = "2010-03-21 00:01:01.943 | 1, Tissa Perera, 10, 0 | -, -, -, - | -, -, -, -\n" + 
				"2010-03-22 00:01:01.943 | 2, Tissa Perera, 10, 0 | 1, Tissa Perera, 9, 0 | -, -, -, -\n" + 
				"2010-03-23 00:01:01.943 | 3, Tissa Perera, 10, 0 | 2, Tissa Perera, 9, 0 | 1, Tissa Perera, 8, 0\n" + 
				"2010-03-24 00:01:01.943 | 4, Tissa Perera, 10, 0 | 3, Tissa Perera, 9, 0 | 2, Tissa Perera, 8, 0\n" + 
				"2010-03-25 00:01:01.943 | 5, Tissa Perera, 10, 0 | 4, Tissa Perera, 9, 0 | 3, Tissa Perera, 8, 0\n";
		String result = testRun("Q1Case1");
		assertEquals(expected, result);
	}
	
	@Test
	public void testQ1Case2() {
		String expected = "2010-04-21 00:01:01.943 | 6, A B, 10, 0 | -, -, -, - | -, -, -, -\n" + 
				"2010-05-01 00:01:01.943 | 7, A B, 10, 0 | -, -, -, - | -, -, -, -\n" + 
				"2010-05-11 00:01:01.943 | 8, A B, 10, 0 | -, -, -, - | -, -, -, -\n" + 
				"2010-05-21 00:01:01.943 | 9, A B, 10, 0 | -, -, -, - | -, -, -, -\n" + 
				"2010-05-31 00:01:01.943 | 10, A B, 10, 0 | -, -, -, - | -, -, -, -\n";
		String result = testRun("Q1Case2");
		assertEquals(expected, result);
	}
	
	@Test
	public void testQ1Case3() {
		String expected = "2010-07-01 00:01:01.943 | 11, C D, 10, 0 | -, -, -, - | -, -, -, -\n" + 
				"2010-07-01 00:01:01.943 | 11, C D, 10, 0 | 17, D F, 10, 0 | -, -, -, -\n" + 
				"2010-07-01 00:01:01.943 | 11, C D, 20, 1 | 17, D F, 10, 0 | -, -, -, -\n" + 
				"2010-07-01 00:01:01.943 | 11, C D, 30, 2 | 17, D F, 10, 0 | -, -, -, -\n" + 
				"2010-07-02 00:01:01.943 | 11, C D, 27, 2 | 17, D F, 19, 1 | -, -, -, -\n" + 
				"2010-07-02 00:01:01.943 | 17, D F, 29, 2 | 11, C D, 27, 2 | -, -, -, -\n" + 
				"2010-07-03 00:01:01.943 | 11, C D, 34, 3 | 17, D F, 26, 2 | -, -, -, -\n" + 
				"2010-07-03 00:01:01.943 | 11, C D, 44, 4 | 17, D F, 26, 2 | -, -, -, -\n" + 
				"2010-07-04 00:01:01.943 | 11, C D, 39, 4 | 17, D F, 33, 3 | -, -, -, -\n" + 
				"2010-07-04 00:01:01.943 | 17, D F, 43, 4 | 11, C D, 39, 4 | -, -, -, -\n" + 
				"2010-07-05 00:01:01.943 | 11, C D, 44, 5 | 17, D F, 38, 4 | -, -, -, -\n" + 
				"2010-07-05 00:01:01.943 | 11, C D, 54, 6 | 17, D F, 38, 4 | -, -, -, -\n";
		String result = testRun("Q1Case3");
		assertEquals(expected, result);
	}
	
	@Test
	public void testQ1Case4() {
		String expected = "2010-08-21 00:01:01.943 | 12, Tissa Perera, 10, 0 | -, -, -, - | -, -, -, -\n" + 
				"2010-08-22 00:01:01.943 | 13, Tissa Perera, 10, 0 | 12, Tissa Perera, 9, 0 | -, -, -, -\n" + 
				"2010-08-23 00:01:01.943 | 14, Tissa Perera, 10, 0 | 13, Tissa Perera, 9, 0 | 12, Tissa Perera, 8, 0\n" + 
				"2010-08-23 00:01:01.943 | 14, Tissa Perera, 20, 1 | 13, Tissa Perera, 9, 0 | 12, Tissa Perera, 8, 0\n" + 
				"2010-08-23 00:02:01.943 | 14, Tissa Perera, 30, 2 | 13, Tissa Perera, 9, 0 | 12, Tissa Perera, 8, 0\n" + 
				"2010-08-23 00:03:01.943 | 14, Tissa Perera, 30, 2 | 12, Tissa Perera, 18, 1 | 13, Tissa Perera, 9, 0\n";
		String result = testRun("Q1Case4");
		assertEquals(expected, result);
	}
	
	@Test
	public void testQ1Case5() {
		String expected = "2010-09-15 00:01:01.943 | 15, C D, 10, 0 | -, -, -, - | -, -, -, -\n" + 
				"2010-09-15 00:01:01.943 | 15, C D, 10, 0 | 16, C D, 10, 0 | -, -, -, -\n" + 
				"2010-10-23 00:03:01.943 | 15, C D, 10, 1 | -, -, -, - | -, -, -, -\n";
		String result = testRun("Q1Case5");
		assertEquals(expected, result);
	}
	
	@Test
	public void testQ1CommentCount() {
		String expected = "2010-02-01 05:12:32.921 | 1039993, Lei Liu, 10, 0 | -, -, -, - | -, -, -, -\n" + 
				"2010-02-08 04:05:20.777 | 1039993, Lei Liu, 14, 1 | -, -, -, - | -, -, -, -\n" + 
				"2010-02-18 08:12:06.616 | 1039993, Lei Liu, 16, 3 | -, -, -, - | -, -, -, -\n" + 
				"2010-02-19 04:05:20.777 | 1039993, Lei Liu, 26, 4 | -, -, -, - | -, -, -, -\n" + 
				"2010-02-19 07:16:06.967 | 1039993, Lei Liu, 35, 5 | -, -, -, - | -, -, -, -\n" + 
				"2010-03-01 07:16:06.967 | -, -, -, - | -, -, -, - | -, -, -, -\n";
		String result = testRun("Q1CommentCount");
		assertEquals(expected, result);
	}
	
	@Test
	public void testQ1PostExpiredComment() {
		String expected = "2010-02-01 05:12:32.921 | 1039993, Lei Liu, 10, 0 | -, -, -, - | -, -, -, -\n";
		String result = testRun("Q1PostExpiredComment");
		assertEquals(expected, result);
	}
	
	@Test
	public void testQ1PostExpiredComment2() {
		String expected = "2010-02-01 05:12:32.921 | 1039993, Lei Liu, 10, 0 | -, -, -, - | -, -, -, -\n" + 
				"2010-02-03 04:05:20.777 | 1039993, Lei Liu, 19, 1 | -, -, -, - | -, -, -, -\n" + 
				"2010-02-12 19:53:43.226 | 299101, Michael Wang, 10, 0 | 1039993, Lei Liu, 1, 1 | -, -, -, -\n" + 
				"2010-02-23 04:05:10.421 | 529360, Wei Zhu, 10, 0 | -, -, -, - | -, -, -, -\n";
		String result = testRun("Q1PostExpiredComment2");
		assertEquals(expected, result);
	}

	public String testRun(String test) {
		
		BlockingQueue<PostCom> blockingQueuePost = new ArrayBlockingQueue<PostCom>(50);
		BlockingQueue<PostCom> blockingQueueComment = new ArrayBlockingQueue<PostCom>(50);
		BlockingQueue<PostCom> blockingQueuePostCom = new ArrayBlockingQueue<PostCom>(100);
		
		File fPost = new File("Tests\\" + test + "\\posts.dat");
		File fComment = new File("Tests\\" + test + "\\comments.dat");
		
		ExecutorService service = Executors.newFixedThreadPool(4);
		
		FileReader pReader;
		Thread pReaderThread = null;
		try {
			pReader = new FileReader(blockingQueuePost, fPost, true);
			pReaderThread = new Thread(pReader);
			service.execute(pReaderThread);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		FileReader cReader;
		Thread cReaderThread = null;
		try {
			cReader = new FileReader(blockingQueueComment, fComment, false);
			cReaderThread = new Thread(cReader);
			service.execute(cReaderThread);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		QueuesSorter sorter = new QueuesSorter(blockingQueuePost, blockingQueueComment, blockingQueuePostCom);
		Thread sorterThread = new Thread(sorter);
		service.execute(sorterThread);

		Processor processor = new Processor(blockingQueuePostCom, true);
		Thread processorThread = new Thread(processor);
		service.execute(processorThread);
		
		Main.shutdownAndAwaitTermination(service);
		
		return processor.getResult();
	}
}
