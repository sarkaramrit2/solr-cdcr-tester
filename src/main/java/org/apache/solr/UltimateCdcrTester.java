package org.apache.solr;


import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class UltimateCdcrTester {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    static String[] strings = new String[5];
    static final String[] FIELDS = new String[]{"order_no_s", "ship_addr_s"};
    private static Random r = new Random();
    private static String C1_ZK;
    private static String C2_ZK;
    private static String col1 = "cluster1";
    private static String col2 = "cluster2";
    private static final String ALL = "*:*";

    public static void main(String args[]) throws IOException, SolrServerException, Exception {
        if (args.length == 0) {
            args = new String[]{"localhost:9983", "localhost:8574", "1", "1"};
            // default indexing batch size: 1024 x 1 = 1024
            // default number of iterations: 200 x 1 = 200
        }
        C1_ZK = args[0]; //set cluster-1 zk
        C2_ZK = args[1]; //set cluster-2 zk
        if (args.length > 4) {
            col1 = args[4]; //setup col 1 name
            col2 = args[5]; //setup col 2 name
        }

        String source_zkHost = C1_ZK; //initial_source
        String target_zkHost = C2_ZK; //initial_target
        CloudSolrClient source_cli = new CloudSolrClient.Builder().withZkHost(source_zkHost).build();
        String source_col = col1;
        source_cli.setDefaultCollection(source_col);
        CloudSolrClient target_cli = new CloudSolrClient.Builder().withZkHost(target_zkHost).build();
        String target_col = col2;
        source_cli.setDefaultCollection(target_col);

        long curr_time = System.currentTimeMillis();
        System.out.println("start-time :: " + curr_time);

        ThreadLocalRandom r = ThreadLocalRandom.current();
        UpdateRequest updateRequest = null;

        // create data for payload
        loadData();

        for (int j = 0; j < 200 * Integer.parseInt(args[2]); j++) {
            //while (true) {
            List<SolrInputDocument> docs = new ArrayList<>();
            int action = r.nextInt(6) % 6;
            // 50% of probability of indexing, and 16.67% of other three ops:
            // delete-by-id, delete-by-query, toggle-cluster

            switch (action) {

                case 0:
                    // delete-by-id
                    String fieldName = FIELDS[r.nextInt(2) % 2];
                    String fieldValue = strings[r.nextInt(5) % 5];
                    String payload = fieldName + ":" + fieldValue;
                    QueryResponse source_resp = source_cli.query(new SolrQuery(payload));
                    if (source_resp.getResults().getNumFound() > 0) {
                        String idToDel = source_resp.getResults().get(0).get("id").toString();
                        System.out.println("deleteByID: " + updateRequest);
                        updateRequest = new UpdateRequest();
                        updateRequest.deleteById(idToDel);
                        source_cli.request(updateRequest, source_col);
                        updateRequest.commit(source_cli, source_col);

                        long start = System.nanoTime();
                        QueryResponse source_resp1 = source_cli.query(new SolrQuery("id:" + idToDel));
                        QueryResponse target_resp1 = null;
                        waitForSync(source_cli, target_cli, payload);
                    }
                    break;

                case 1:
                    // delete-by-query
                    updateRequest = new UpdateRequest();
                    String fieldName1 = FIELDS[r.nextInt(2) % 2];
                    String fieldValue1 = strings[r.nextInt(5) % 5];
                    String payload1 = fieldName1 + ":" + fieldValue1;
                    updateRequest.deleteByQuery(payload1);
                    System.out.println("deleteByQuery: " + updateRequest);

                    source_cli.request(updateRequest, source_col);
                    updateRequest.commit(source_cli, source_col);

                    waitForSync(source_cli, target_cli, payload1);
                    break;

                case 2:
                    // toggle source and target
                    if (source_zkHost.equals(C1_ZK)) { // make C2_ZK primary
                        source_zkHost = C2_ZK;
                        source_col = col2;
                        target_zkHost = C1_ZK;
                        target_col = col1;
                    } else { // make C1_ZK primary
                        source_zkHost = C1_ZK;
                        source_col = col1;
                        target_zkHost = C2_ZK;
                        target_col = col2;
                    }
                    System.out.println("toggle direction: " + source_col + " | " + target_col);

                    source_cli = new CloudSolrClient.Builder().withZkHost(source_zkHost).build();
                    source_cli.setDefaultCollection(source_col);
                    target_cli = new CloudSolrClient.Builder().withZkHost(target_zkHost).build();
                    source_cli.setDefaultCollection(target_col);
                    break;

                default:
                    // index
                    for (int i = 0; i < 1024 * Integer.parseInt(args[3]); i++) {
                        SolrInputDocument document = new SolrInputDocument();
                        document.addField("id", UUID.randomUUID().toString());
                        document.addField("member_id_i", r.nextInt(5) % 5);
                        document.addField("subtotal_i", 1024 + r.nextInt(5) % 5);
                        document.addField("quantity_l", Math.abs(r.nextLong() % 5));
                        document.addField("order_no_s", strings[r.nextInt(5) % 5]);
                        document.addField("ship_addr_s", strings[r.nextInt(5) % 5]);

                        docs.add(document);
                    }
                    updateRequest = new UpdateRequest();
                    updateRequest.add(docs);

                    System.out.println("index: " + updateRequest);
                    source_cli.request(updateRequest, source_col);
                    updateRequest.commit(source_cli, source_col);

                    docs.clear();
                    break;
            }
            Thread.sleep(500);
        }

        long end_time = System.currentTimeMillis();
        System.out.println("start-time :: " + end_time);
        System.out.println("total time spend :: " + (end_time - curr_time));

        System.exit(0);
    }

    // create sentences for data to index
    private static String createSentance(int numWords) {
        //Sentence with numWords and 3-7 letters in each word
        StringBuilder sb = new StringBuilder(numWords * 2);
        for (int i = 0; i < numWords; i++) {
            sb.append(TestUtil.randomSimpleString(r, 1, 1));
        }
        return sb.toString();
    }

    // create data for index
    private static void loadData() {
        // to index payload
        for (int i = 0; i < 5; i++) {
            strings[i] = createSentance(1);
        }
    }

    // helper function to all cluster
    private static boolean docsInSync(CloudSolrClient src, CloudSolrClient tar) throws Exception {
        if (src.query(new SolrQuery(ALL)).getResults().getNumFound() == (tar.query(new SolrQuery(ALL)).getResults().getNumFound())) {
            return true;
        }
        return false;
    }

    // helper function to validate sync
    private static void waitForSync(CloudSolrClient source_cli, CloudSolrClient target_cli, String payload) throws Exception {
        long start = System.nanoTime();
        QueryResponse source_resp = source_cli.query(new SolrQuery(payload));
        QueryResponse target_resp = null;
        while (System.nanoTime() - start <= TimeUnit.NANOSECONDS.convert(240, TimeUnit.SECONDS)) {
            Thread.sleep(2000); // pause
            if (!docsInSync(source_cli, target_cli)) {
                continue;
            }
            target_cli.commit();
            target_resp = target_cli.query(new SolrQuery(payload));
            if (target_resp.getResults().getNumFound() == source_resp.getResults().getNumFound()) {
                break;
            }
        }
        if (target_resp != null) {
            assert target_resp.getResults().getNumFound() == source_resp.getResults().getNumFound();
        }
    }

}
