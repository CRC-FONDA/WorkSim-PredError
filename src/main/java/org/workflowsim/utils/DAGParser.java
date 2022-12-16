package org.workflowsim.utils;

import org.workflowsim.Task;
import org.workflowsim.WorkflowParser;

import java.io.BufferedReader;
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
    private Map<String, String> machine_id_name;

    public DAGParser(){
        machine_id_name = new HashMap<>();
        parser = new WorkflowParser(-1);
        parser.parse();
        workflow_DAG = parser.getTaskList();
        get_machine_speed(Parameters.get_csv_path());
        parse_csv_file(Parameters.get_csv_path() + "averageRuntimesPredictionBase.csv");
        // init_task_priority_ranking();
        for (Task t : workflow_DAG){
            System.out.println("DEBUGGING: cloudletid=" + t.getCloudletId() + " | type=" + t.getType());
        }
    }

    public void get_machine_speed(String path_to_machines){
        ArrayList<String[]> machine_ids_names = parse_csv_file(path_to_machines + "nodeConfigs.csv");
        int id_idx = get_string_index("id", machine_ids_names.get(0));
        int name_idx = get_string_index("type", machine_ids_names.get(0));
        for (int i = 1; i < machine_ids_names.size(); i++){
            machine_id_name.put(machine_ids_names.get(i)[id_idx], machine_ids_names.get(i)[name_idx]);
        }
        for (String key : machine_id_name.keySet()){
            System.out.println(key + " | " + machine_id_name.get(key));
        }
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
        if (u.getChildList().isEmpty()){
            return u.getProcessingCost();
        }
        double rank = 0.0;
        for (Task v : u.getChildList()){
            rank = Math.max(rank, DFS(v, m));
        }
        m.put(u, rank + u.getProcessingCost());
        System.out.println("DEBUGGING: taskrank=" + m.get(u));
        return rank + u.getProcessingCost();
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
