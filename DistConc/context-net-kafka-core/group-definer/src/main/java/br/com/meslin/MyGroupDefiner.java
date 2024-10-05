package br.com.meslin;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.function.Consumer;

import org.openstreetmap.gui.jmapviewer.Coordinate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import br.com.meslin.auxiliar.StaticLibrary;
import br.com.meslin.model.Inspector;
import br.com.meslin.model.Region;
import br.com.meslin.model.SamplePredicate;
import ckafka.data.Swap;
import main.java.ckafka.GroupDefiner;
import main.java.ckafka.GroupSelection;

public class MyGroupDefiner implements GroupSelection {
    /** Logger */
    final Logger logger = LoggerFactory.getLogger(GroupDefiner.class);
    /** Array of regions */
    private List<Region> regionList;
    /** JMapViewer-based map */
    //private GeographicMap map;
    /** A list of inspector from SeFaz */
    private List<Inspector> inspectorList;

    public MyGroupDefiner() {
        /*
         * Getting regions
         */
        String workDir = System.getProperty("user.dir");
        System.out.println("Working Directory = " + workDir);
        String fullFilename = workDir + "/Bairros/RioDeJaneiro.txt";
        List<String> lines = StaticLibrary.readFilenamesFile(fullFilename);
        // reads each region file
        this.regionList = new ArrayList<Region>();
        for(String line : lines) {
            int regionNumber = Integer.parseInt(line.substring(0, line.indexOf(",")).trim());
            String filename = line.substring(line.indexOf(",")+1).trim();
            Region region = StaticLibrary.readRegion(filename, regionNumber);
            this.regionList.add(region);
        }

        ObjectMapper objectMapper = new ObjectMapper();
        Swap swap = new Swap(objectMapper);
        new GroupDefiner(this, swap);
    }

    public static void main(String[] args) {
    	// creating missing environment variable
		Map<String,String> env = new HashMap<String, String>();
		env.putAll(System.getenv());
		if(System.getenv("gd.one.consumer.topics") == null) 			env.put("gd.one.consumer.topics", "GroupReportTopic");
		if(System.getenv("gd.one.consumer.auto.offset.reset") == null) 	env.put("gd.one.consumer.auto.offset.reset", "latest");
		if(System.getenv("gd.one.consumer.bootstrap.servers") == null) 	env.put("gd.one.consumer.bootstrap.servers", "127.0.0.1:9092");
		if(System.getenv("gd.one.consumer.group.id") == null) 			env.put("gd.one.consumer.group.id", "gw-gd");
		if(System.getenv("gd.one.producer.bootstrap.servers") == null) 	env.put("gd.one.producer.bootstrap.servers", "127.0.0.1:9092");
		if(System.getenv("gd.one.producer.retries") == null) 			env.put("gd.one.producer.retries", "3");
		if(System.getenv("gd.one.producer.enable.idempotence") == null)	env.put("gd.one.producer.enable.idempotence", "true");
		if(System.getenv("gd.one.producer.linger.ms") == null) 			env.put("gd.one.producer.linger.ms", "1");
		try {
			StaticLibrary.setEnv(env);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// creating new GroupDefiner
        new MyGroupDefiner();
    }

    /**
     * Conjunto com todos os grupos que esse GroupDefiner controla.
     */
    public Set<Integer> groupsIdentification() {
        Set<Integer> setOfGroups = new HashSet<Integer>();
        for (Region region : regionList) {
            setOfGroups.add(region.getNumber());
        }
        return setOfGroups;
    }

    /**
     * Conjunto com todos os grupos relativos a esse contextInfo.
     * Somente grupos controlados por esse GroupDefiner.
     * @param contextInfo context info
     */
    public Set<Integer> getNodesGroupByContext(ObjectNode contextInfo) {
        Inspector inspector = null;
        Set<Integer> setOfGroups = new HashSet<Integer>();
        double latitude = Double.parseDouble(String.valueOf(contextInfo.get("latitude")));
        double longitude = Double.parseDouble(String.valueOf(contextInfo.get("longitude")));
        try {
            inspector = new Inspector(String.valueOf(contextInfo.get("date")), latitude, longitude, String.valueOf(contextInfo.get("ID")));
        } catch (NumberFormatException | ParseException e) {
            e.printStackTrace(); 
        }
        inspectorList.removeIf(new SamplePredicate(inspector.getUuid()));
        inspectorList.add(inspector);
        Coordinate coordinate = new Coordinate(latitude, longitude);
        for (Region region : regionList) {
            if(region.contains(coordinate)) {
                setOfGroups.add(region.getNumber());
            }
        }
        return setOfGroups;
    }
    
    /**
     * 
     */
    public String kafkaConsumerPrefix()  {
        return "gd.one.consumer";
    }

    /**
     * 
     */
    public String kafkaProducerPrefix() {
        return "gd.one.producer";
    }
}
