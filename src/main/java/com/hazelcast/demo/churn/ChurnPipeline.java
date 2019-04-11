package com.hazelcast.demo.churn;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.CompletableFuture;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.rlab.entity.ContractInfo;
import com.rlab.entity.CustomerUsageDetails;

public class ChurnPipeline {

	private static final String CONTRACT_INFO = "contract-info";

	private static Logger log = LogManager.getLogger(ChurnPipeline.class);
	
	private static IMap callDataMap;
	private static IMap contractInfoMap;
	private static HazelcastInstance hz;
	
	private static String CALL_DATA_MAP = "call-data";
	
	//
	// This main is for testing
	//
	public static  void main(String []args) {
        JetInstance jet = Jet.newJetInstance();
		hz = jet.getHazelcastInstance();
		callDataMap = hz.getMap(CALL_DATA_MAP);
		contractInfoMap = hz.getMap(CONTRACT_INFO);
		
		try {
			loadCustomerUsage();
			loadContractInfo();
		} catch(IOException x) {
			log.error("loading failed - exiting");
			System.exit(1);;
		}
		
		Pipeline pipeline = buildPipeline();
		
        try {
            jet.newJob(pipeline).join();
        } finally {
            Jet.shutdownAll();
        }
		
	}
	
	private static Pipeline buildPipeline() {
		Pipeline result = Pipeline.create();
		
		ContextFactory<Tuple2<IMapJet<String, CustomerUsageDetails>, IMapJet<String, ContractInfo>>> contextFactory 
			= ContextFactory.withCreateFn(jet -> Tuple2.tuple2(jet.<String,CustomerUsageDetails>getMap(CALL_DATA_MAP),jet.<String,ContractInfo>getMap(CONTRACT_INFO)));
		
		ContextFactory<ScoringContext> scoringContextFactory = ContextFactory.withCreateFn(jet -> new ScoringContext());
		
		result.drawFrom(Sources.files("batch_input"))
			.map(phoneNumber -> phoneNumber.split(","))
			.map(array -> CustomerUsageDetails.createKey(array[0], array[1]))
			.mapUsingContextAsync(contextFactory,
					 (tuple, item) -> { 
						 CompletableFuture<CustomerUsageDetails> future1 =  Util.toCompletableFuture(tuple.f0().getAsync(item));
						 CompletableFuture<ContractInfo> future2 =  Util.toCompletableFuture(tuple.f1().getAsync(item));
						 return CompletableFuture.allOf(future1,future2).thenApply( x -> Tuple2.tuple2(future1.join(),future2.join()));
					 })
			.filter( item -> item.f0()!= null && item.f1() != null)
			.mapUsingContext(scoringContextFactory,(scoringContext, item) -> scoringContext.predictChurn(item.f1(), item.f0()) )
			.drainTo(Sinks.logger());
		
		return result;
	}

	private static void loadCustomerUsage() throws IOException {
		int count = 0;
		try(BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("CallsData.csv")))){
			String line = reader.readLine();
			line = reader.readLine();
			while(line != null) {
				String []fields = line.split(",");
				
				CustomerUsageDetails ci = new CustomerUsageDetails(Integer.parseInt(fields[0]),
	                    Float.parseFloat(fields[1]),
	                    Float.parseFloat(fields[2]),
	                    Float.parseFloat(fields[3]),
	                    Float.parseFloat(fields[4]),
	                    
	                    Integer.parseInt(fields[5]),
	                    
	                    Integer.parseInt(fields[6]),
	                    Float.parseFloat(fields[7]),
	                    
	                    Integer.parseInt(fields[8]),
	                    Float.parseFloat(fields[9]),
	                    
	                    Integer.parseInt(fields[10]),
	                    Float.parseFloat(fields[11]),
	                    
	                    Integer.parseInt(fields[12]),
	                    Float.parseFloat(fields[13]),
	                    
	                    fields[14],
	                 	fields[15]
	                    );
			
				callDataMap.put(ci.getKey(), ci);
				++count;
				
				line = reader.readLine(); 
			}

			System.out.println("loaded " + count + " customer usage entries");
		}
	}
		

	private static void loadContractInfo() throws IOException {
		int count = 0;
		try(BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("ContractData.csv")))){
			String line = reader.readLine();
			line = reader.readLine();
			while(line != null) {
				String []fields = line.split(",");
				
				ContractInfo ci = new ContractInfo(Integer.parseInt(fields[0]),
                        Integer.parseInt(fields[1]),
                        Integer.parseInt(fields[2]),
                        Integer.parseInt(fields[3]),
                        fields[4],
                        Integer.parseInt(fields[5]),
                        fields[6]);

				
				contractInfoMap.put(ci.getKey(), ci);
				++count;
				
				line = reader.readLine(); 
			}

		}
		
		System.out.println("loaded " + count + " contract data entries");
		
	}
}
