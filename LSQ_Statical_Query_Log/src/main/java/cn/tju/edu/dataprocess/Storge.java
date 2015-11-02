package cn.tju.edu.dataprocess;

import java.io.File;
import java.io.IOException;

import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.nativerdf.NativeStore;

import cn.tju.edu.util.FileUtil;

public class Storge {
	private File dataDir;
	private SailRepository repository;
	private RepositoryConnection connection;
	
	public RepositoryConnection getConnection() {
		return connection;
	}

	public Storge(String dataDirPath) {
		dataDir = FileUtil.IfNotExistThenCreateDirectory(dataDirPath);
		repository = new SailRepository(new NativeStore(dataDir));
		try {
			repository.initialize();
			connection = repository.getConnection();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public boolean StorgeRDFFromFile(String RDFFilePath) {
		System.out.println("begin storge from file " + RDFFilePath + " ......");
		
		File RDFDataFile = new File(RDFFilePath);
		
		try {
			connection.add(RDFDataFile, "", RDFFormat.TURTLE);
		} catch (RDFParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("storge end from" + RDFFilePath + "......");
		
		return true;
	}
}
