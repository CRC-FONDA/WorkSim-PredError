package org.workflowsim.utils;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.workflowsim.MetaGetter;
import org.workflowsim.Task;
import org.workflowsim.WorkflowParser;
import org.xml.sax.SAXException;


import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class DAGParser {

    // parser to parse the .xml file containing DAG structure
    private WorkflowParser parser;
    // these two fields are needed to compute the recursive ranking of tasks.
    private List<Task> workflow_DAG;
    // these two fields are needed to compute the recursive ranking of tasks.
    private Map<Task, Double> task_ranking;
    /**
     * this map stores the mapping of machine id and corresponding name in the csv files of "tested_runtimes".
     */
    private Map<String, Double> machine_name_runtime;

    private Map<String, Double> relative_machine_speed;

    public DAGParser(){
        machine_name_runtime = new HashMap<>();
        parser = new WorkflowParser(-1);
        parser.parse();
        workflow_DAG = parser.getTaskList();
        relative_machine_speed = new HashMap<>();
        filter_benchmark_results("Test: MD5");
        parseDaxXml(MetaGetter.getRealdaxPath());
        init_task_priority_ranking();
    }

    public void parseDaxXml(String realDaxPath){

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        try {

            // optional, but recommended
            // process XML securely, avoid attacks like XML External Entities (XXE)
            dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);

            // parse XML file
            DocumentBuilder db = dbf.newDocumentBuilder();

            Document doc = db.parse(new File(realDaxPath + MetaGetter.getWorkflow() + ".xml"));

            // optional, but recommended
            doc.getDocumentElement().normalize();

            NodeList list = doc.getElementsByTagName("job");
            for (int i = 0; i < list.getLength(); i++){
                Node node = list.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element element = (Element) node;
                    String name = element.getAttribute("name");
                    String runtime = element.getAttribute("runtime");
                    machine_name_runtime.put(name, Double.parseDouble(runtime));
                }
            }
        }
        catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }

    }



    public void filter_benchmark_results(String benchmark){
        ArrayList<String[]> benchmark_results = parse_csv_file(Parameters.get_csv_path() + "nodeBenchmarks.csv");
        int benchmark_idx = get_string_index("benchmarkType", benchmark_results.get(0));
        int benchmark_result = get_string_index("result", benchmark_results.get(0));
        int id_idx = get_string_index("nodeConfig", benchmark_results.get(0));
        boolean first_found = false;
        String first_found_key = "";
        for (String[] entry : benchmark_results){
            if (entry[benchmark_idx].equals(benchmark)){
                if (!first_found){
                    first_found_key = entry[id_idx];
                    first_found = true;
                    relative_machine_speed.put(entry[id_idx], Double.parseDouble(entry[benchmark_result]));
                }
                else {
                    relative_machine_speed.put(entry[id_idx], Double.parseDouble(entry[benchmark_result]) / relative_machine_speed.get(first_found_key));
                }
            }
        }
        relative_machine_speed.put(first_found_key, 1.0);
    }

    public ArrayList<String[]> parse_csv_file(String path){
        ArrayList<String[]> entries = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                entries.add(values);
            }
        }
        catch (IOException e) {
            System.out.println("Error parsing the csv file.");
            e.printStackTrace();
        }
        return entries;
    }

    /**
     * auxiliary method to get the first index in an array that contains the supplied pattern.
     * @param pattern the pattern to search for
     * @param A the target array
     * @return the index of the pattern, -1 if not present.
     */
    public int get_string_index(String pattern, String[] A){
        for (int i = 0; i < A.length; i++){
            if (A[i].equals(pattern))
                return i;
        }
        System.out.println("Error, pattern not found.");
        return -1;
    }

    /**
     * returns the recursive task ranking map from a workflowscheduler object.
     * @return the task ranking.
     */
    public Map<Task, Double> get_task_ranking(){
        return task_ranking;
    }

    /**
     * a DFS method to compute the length of the longest computation path for any node in the DAG.
     * @param u the starting node.
     * @param m the map to cache results in.
     * @return the computed rank for every node.
     */
    public double DFS(Task u, Map<Task, Double> m){
        if (m.containsKey(u) && m.get(u) != -1.0)
            return m.get(u);
        double rank = 0.0;
        for (Task v : u.getChildList()){
            rank = Math.max(rank, DFS(v, m));
        }
        m.put(u, rank + machine_name_runtime.get(u.getType()));
        return m.get(u);
    }


    /**
     * assigns the maximum of cumulative remaining computation time of a node and any possible
     * computation path until a leaf node.
     */
    public void init_task_priority_ranking(){
        Map<Task, Double> m = new HashMap<>();
        for (Task t : workflow_DAG){
            m.put(t, -1.0);
        }
        for (Task u : workflow_DAG){
            if (u.getParentList().isEmpty()){
                DFS(u, m);
            }
        }
        task_ranking = m;
    }
}
