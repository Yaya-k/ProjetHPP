package projectQuery1;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import entities.PostCom;
import traitement.FileReader;
import traitement.Processor;
import traitement.QueuesSorter;
import traitement.ReaderSorter;

@State(Scope.Benchmark)
public class BenchmarkMainRun {
	
	private String test;
	
	private BlockingQueue<PostCom> blockingQueuePost;
	private BlockingQueue<PostCom> blockingQueueComment;
	private BlockingQueue<PostCom> blockingQueuePostCom;
	
	private File fPost;	
	private File fComment;
	
	private FileReader pReader;
	private Thread pReaderThread;

	private FileReader cReader;
	private Thread cReaderThread;
	
	private QueuesSorter sorter;
	private Thread sorterThread;
	
	private ReaderSorter readerSorter;
	private Thread readerSorterThread;
	
	private Processor processor;
	private Thread processorThread;
	
	private Processor processor2T;
	private Thread processor2TThread;
	
	private ExecutorService service;
	private ExecutorService service2T;
	
	@Setup(Level.Invocation)
	public void Setup() {
		
		this.test = "Q1BigTest";
		
		this.blockingQueuePost = new ArrayBlockingQueue<PostCom>(50);
		this.blockingQueueComment = new ArrayBlockingQueue<PostCom>(50);
		this.blockingQueuePostCom = new ArrayBlockingQueue<PostCom>(100);
		
		this.fPost = new File("Tests\\" + test + "\\posts.dat");
		this.fComment = new File("Tests\\" + test + "\\comments.dat");
		
		this.service = Executors.newFixedThreadPool(4);
		this.service2T = Executors.newFixedThreadPool(2);

		try {
			this.pReader = new FileReader(this.blockingQueuePost, this.fPost, true);
			this.pReaderThread = new Thread(this.pReader);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		try {
			this.cReader = new FileReader(this.blockingQueueComment, this.fComment, false);
			this.cReaderThread = new Thread(this.cReader);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		this.sorter = new QueuesSorter(this.blockingQueuePost, this.blockingQueueComment, this.blockingQueuePostCom);
		this.sorterThread = new Thread(this.sorter);
		
		try {
			readerSorter = new ReaderSorter(fPost, fComment, blockingQueuePostCom);
			readerSorterThread = new Thread(readerSorter);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		this.processor = new Processor(this.blockingQueuePostCom, true);
		this.processorThread = new Thread(this.processor);
		
		this.processor2T = new Processor(this.blockingQueuePostCom, true);
		this.processor2TThread = new Thread(this.processor2T);
	}

	@Fork(value = 2)
	@Measurement(iterations = 5)
	@Warmup(iterations = 5)
	@Timeout(time = 30)
	@BenchmarkMode(Mode.All)
	@OutputTimeUnit(TimeUnit.MILLISECONDS)
	@Benchmark
    public void benchmarkMainRun() {
				
		this.service.execute(this.pReaderThread);
		this.service.execute(this.cReaderThread);
		this.service.execute(this.sorterThread);
		this.service.execute(this.processorThread);
		
		Main.shutdownAndAwaitTermination(this.service);
	}
	
	@Fork(value = 2)
	@Measurement(iterations = 5)
	@Warmup(iterations = 5)
	@Timeout(time = 30)
	@BenchmarkMode(Mode.All)
	@OutputTimeUnit(TimeUnit.MILLISECONDS)
	@Benchmark
    public void benchmarkMain2TRun() {
				
		this.service2T.execute(this.readerSorterThread);
		this.service2T.execute(this.processor2TThread);
		
		Main.shutdownAndAwaitTermination(this.service2T);
	}
	
	public static void main (String[] args) throws Exception {
		org.openjdk.jmh.Main.main(args);
	}
}
