package com.omor_rci.elastic_search_example;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.client.RestClient;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import com.google.gson.Gson;

public class CSVReader {

	// The config parameters for the connection
	private static final String HOST = "localhost";
	private static final int PORT_ONE = 9200;
	private static final int PORT_TWO = 9201;
	private static final String SCHEME = "http";

	private static RestHighLevelClient restHighLevelClient;
	
	private static final String INDEX = "temperaturedata";
	private static final String TYPE = "temperature";

	/**
	 * Implemented Singleton pattern here so that there is just one connection at a
	 * time.
	 * 
	 * @return RestHighLevelClient
	 */
	private static synchronized RestHighLevelClient makeConnection() {

		if (restHighLevelClient == null) {
			restHighLevelClient = new RestHighLevelClient(
					RestClient.builder(new HttpHost(HOST, PORT_ONE, SCHEME), new HttpHost(HOST, PORT_TWO, SCHEME)));
		}

		return restHighLevelClient;
	}

	private static synchronized void closeConnection() throws IOException {
		restHighLevelClient.close();
		restHighLevelClient = null;
	}

	public static void main(String[] args) throws IOException {

		System.out.println("Working with elastic search!");
		makeConnection();

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

		String csvFile = "/Users/mdomor.faruque/Tools/data/temperatures.csv";
		BufferedReader bReader = null;
		String line = "";
		String csvSplitBy = ",";
		String[] columnName;
		String indx = "{\"index\":{}}";

		try {
			bReader = new BufferedReader(new FileReader(csvFile));
			columnName = bReader.readLine().split(csvSplitBy);
			while ((line = bReader.readLine()) != null) {

				Map<String, Object> obj = new HashMap<String, Object>();

				String[] lineWords = line.split(csvSplitBy);

				for (int i = 0; i < lineWords.length; i++) {
					obj.put(columnName[i], lineWords[i]);
				}
				list.add(obj);

			}

			Gson gson = new Gson();
			// String json = gson.toJson(list.get(0));
			String json;

			for (int i = 0; i < list.size(); i++) {
				json = gson.toJson(list.get(i));


				IndexRequest request = new IndexRequest(INDEX, TYPE);
				request.source(json, XContentType.JSON);

				try {
					System.out.println("1");
					IndexResponse response = restHighLevelClient.index(request);
				} catch (ElasticsearchException e) {
					System.out.println("2");
					e.getDetailedMessage();
				} catch (java.io.IOException ex) {
					System.out.println("3");
					ex.getLocalizedMessage();
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (bReader != null) {
				try {
					bReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		closeConnection();
	}

}
