package com.hazelcast.demo.churn;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithKey;
import com.rlab.entity.BatchSummary;
import com.rlab.entity.ContractInfo;
import com.rlab.entity.CustomerUsageDetails;
import com.rlab.kafka.message.KMessage;

public class ChurnPipeline {

	private static final String CONTRACT_INFO = "contract-info";

	private static Logger log = LogManager.getLogger(ChurnPipeline.class);

	private static IMap callDataMap;
	private static IMap contractInfoMap;
	//private static HazelcastInstance hz;

	private static String CALL_DATA_MAP = "call-data";
	private static String BATCH_SUMMARY_MAP = "batch-summary";

	//
	// This main is for testing
	//
	public static void main(String[] args) {
		JetInstance jet = Jet.newJetInstance();
		//hz = jet.getHazelcastInstance();
		callDataMap = remoteHazelcast().getMap(CALL_DATA_MAP);
		contractInfoMap = remoteHazelcast().getMap(CONTRACT_INFO);
		IMap<String, BatchSummary> bsMap = remoteHazelcast().getMap("batch_summary");

		try {
			loadCustomerUsage();
			loadContractInfo();
		} catch (IOException x) {
			log.error("loading failed - exiting");
			System.exit(1);
			;
		}

		Pipeline pipeline = buildPipeline();

		try {
			jet.newJob(pipeline).join();

			for (Entry<String, BatchSummary> k : bsMap.entrySet()) {
				System.out.println("key=" + k.getKey() + "  val=" + k.getValue());
			}

		} finally {
			Jet.shutdownAll();
		}

	}

	private static HazelcastInstance hzClient = null;

	private static HazelcastInstance remoteHazelcast() {
		if(hzClient == null) {
		ClientConfig cc = new ClientConfig();
		cc.getNetworkConfig().addAddress("localhost");
		hzClient=HazelcastClient.newHazelcastClient(cc);
		}
		return hzClient;
	}

	private static Pipeline buildPipeline() {
		ClientConfig cc = new ClientConfig();
		cc.getNetworkConfig().addAddress("localhost");
		
		String client = "localhost";

		Pipeline result = Pipeline.create();

		ContextFactory<Tuple2<IMap<String, CustomerUsageDetails>, IMap<String, ContractInfo>>> contextFactory = ContextFactory
				.withCreateFn(
						jet -> Tuple2.tuple2(remoteHazelcast().<String, CustomerUsageDetails>getMap(CALL_DATA_MAP),
								remoteHazelcast().<String, ContractInfo>getMap(CONTRACT_INFO)));

		ContextFactory<ScoringContext> scoringContextFactory = ContextFactory.withCreateFn(jet -> new ScoringContext());

		AggregateOperation1<Tuple2<KMessage, String>, ?, BatchSummary> aggregateOp = AggregateOperations.allOf(
				AggregateOperations.mapping(item -> item.f1(), AggregateOperations.pickAny()),
				AggregateOperations.counting(), // items in group
				Utils.filtering(item -> item.f0().getAttributes().get("Result").equals("0"),
						AggregateOperations.counting()),
				BatchSummary::newBatchSummary); // mapper

		// note: if you are using grouping the output of an aggregate is Entry<K,V>, NOT
		// V

		StreamStage<Entry<String, BatchSummary>> stage = 
		result.drawFrom(Sources.filesBuilder("batch_input").buildWatcher((filename, line) -> line + "," + filename.substring(0,filename.length() -4)))
			.withoutTimestamps()
			.map(phoneNumber -> phoneNumber.split(","))
			.map(array -> Tuple2.tuple2(CustomerUsageDetails.createKey(array[0], array[1]), array[2])) // (key, filename)																										
			.mapUsingContextAsync(contextFactory, (tuple, item) -> {
					CompletableFuture<CustomerUsageDetails> future1 = Util
							.toCompletableFuture(tuple.f0().getAsync(item.f0()));
					CompletableFuture<ContractInfo> future2 = Util.toCompletableFuture(tuple.f1().getAsync(item.f0()));
					return CompletableFuture.allOf(future1, future2)
							.thenApply(x -> Tuple3.tuple3(future1.join(), future2.join(), item.f1()));
				}) // (CustomerUsageDetails,ContractInfo, filename)
				.filter(item -> item.f0() != null && item.f1() != null) // filter out entries where either is null
				.mapUsingContext(scoringContextFactory,
						(scoringContext, item) -> Tuple2.tuple2(scoringContext.predictChurn(item.f1(), item.f0()),
								item.f2())) // (KMesg, filename)
				.groupingKey(item -> item.f1()).rollingAggregate(aggregateOp); // BatchSummary by batch
			
		stage.drainTo(Sinks.remoteMap("batch_summary", cc));
		stage.drainTo(Sinks.logger());


		return result;
	}

	private static void loadCustomerUsage() throws IOException {
		int count = 0;
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("CallsData.csv")))) {
			String line = reader.readLine();
			line = reader.readLine();
			while (line != null) {
				String[] fields = line.split(",");

				CustomerUsageDetails ci = new CustomerUsageDetails(Integer.parseInt(fields[0]),
						Float.parseFloat(fields[1]), Float.parseFloat(fields[2]), Float.parseFloat(fields[3]),
						Float.parseFloat(fields[4]),

						Integer.parseInt(fields[5]),

						Integer.parseInt(fields[6]), Float.parseFloat(fields[7]),

						Integer.parseInt(fields[8]), Float.parseFloat(fields[9]),

						Integer.parseInt(fields[10]), Float.parseFloat(fields[11]),

						Integer.parseInt(fields[12]), Float.parseFloat(fields[13]),

						fields[14], fields[15]);

				callDataMap.put(ci.getKey(), ci);
				++count;

				line = reader.readLine();
			}

			System.out.println("loaded " + count + " customer usage entries");
		}
	}

	private static void loadContractInfo() throws IOException {
		int count = 0;
		try (BufferedReader reader = new BufferedReader(
				new InputStreamReader(new FileInputStream("ContractData.csv")))) {
			String line = reader.readLine();
			line = reader.readLine();
			while (line != null) {
				String[] fields = line.split(",");

				ContractInfo ci = new ContractInfo(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]),
						Integer.parseInt(fields[2]), Integer.parseInt(fields[3]), fields[4],
						Integer.parseInt(fields[5]), fields[6]);

				contractInfoMap.put(ci.getKey(), ci);
				++count;

				line = reader.readLine();
			}

		}

		System.out.println("loaded " + count + " contract data entries");

	}
}
