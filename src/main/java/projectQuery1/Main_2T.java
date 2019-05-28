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
import traitement.Processor;
import traitement.ReaderSorter;

public class Main_2T {

	public static void main(String[] args) throws InterruptedException {
		
		System.out.println("En attente d'une entree pour lancer le programme");
		Scanner scan = new Scanner(System.in);
		scan.nextLine();
		scan.close();
		
		String test = "Q1BigTest";
		
		BlockingQueue<PostCom> blockingQueuePostCom = new ArrayBlockingQueue<PostCom>(100);
		
		File fPost = new File("Tests\\" + test + "\\posts.dat");
		File fComment = new File("Tests\\" + test + "\\comments.dat");
		
		ExecutorService service = Executors.newFixedThreadPool(2);
		
		ReaderSorter readerSorter;
		Thread readerSorterThread = null;
		try {
			readerSorter = new ReaderSorter(fPost, fComment, blockingQueuePostCom);
			readerSorterThread = new Thread(readerSorter);
			service.execute(readerSorterThread);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		

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
