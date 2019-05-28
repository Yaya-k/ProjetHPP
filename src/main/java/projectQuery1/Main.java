package projectQuery1;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import entities.PostCom;
import traitement.FileReader;
import traitement.Processor;
import traitement.QueuesSorter;

public class Main {

	public static void main(String[] args) throws InterruptedException {
		
		System.out.println("En attente d'une entree pour lancer le programme");
		Scanner scan = new Scanner(System.in);
		scan.nextLine();
		scan.close();
		
		String test = "Q1BigTest";
		
		BlockingQueue<PostCom> blockingQueuePost = new ArrayBlockingQueue<PostCom>(50);
		BlockingQueue<PostCom> blockingQueueComment = new ArrayBlockingQueue<PostCom>(50);
		BlockingQueue<PostCom> blockingQueuePostCom = new ArrayBlockingQueue<PostCom>(100);
		
		File fPost = new File("Tests\\"+test + "\\posts.dat");
		File fComment = new File("Tests\\"+test + "\\comments.dat");
		
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

		Processor processor = new Processor(blockingQueuePostCom);
		Thread processorThread = new Thread(processor);
		service.execute(processorThread);
		
		shutdownAndAwaitTermination(service);
	}
	
	static void shutdownAndAwaitTermination(ExecutorService pool) {
		pool.shutdown();
		try {
			if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
				pool.shutdownNow();
				if (!pool.awaitTermination(60, TimeUnit.SECONDS))
					System.err.println("Pool did not terminate");
			}
		} catch (InterruptedException ie) {
			pool.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}

}
