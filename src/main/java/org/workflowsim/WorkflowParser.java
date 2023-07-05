/**
 * Copyright 2012-2013 University Of Southern California
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.workflowsim;

import java.io.File;
import java.io.IOException;
import java.util.*;

import com.jayway.jsonpath.internal.filter.ValueNodes;
import org.cloudbus.cloudsim.Log;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.Parameters.FileType;
import org.workflowsim.utils.ReplicaCatalog;

/**
 * WorkflowParser parse a DAX into tasks so that WorkflowSim can manage them
 *
 * @author Weiwei Chen
 * @date Aug 23, 2013
 * @date Nov 9, 2014
 * @since WorkflowSim Toolkit 1.0
 */
public final class WorkflowParser {

    /**
     * The path to DAX file.
     */
    private final String daxPath;
    /**
     * The path to DAX files.
     */
    private final List<String> daxPaths;
    /**
     * All tasks.
     */
    private List<Task> taskList;
    /**
     * User id. used to create a new task.
     */
    private final int userId;

    /**
     * current job id. In case multiple workflow submission
     */
    private int jobIdStartsFrom;

    /**
     * Gets the task list
     *
     * @return the task list
     */
    @SuppressWarnings("unchecked")
    public List<Task> getTaskList() {
        return taskList;
    }

    /**
     * Sets the task list
     *
     * @param taskList the task list
     */
    protected void setTaskList(List<Task> taskList) {
        this.taskList = taskList;
    }

    /**
     * Map from task name to task.
     */
    protected Map<String, Task> mName2Task;

    /**
     * Map from task name to list of names of parent tasks
     */
    protected Map<String, List<String>> mName2pNames;

    /**
     * Map from task name to list of names of child tasks
     */
    protected Map<String, List<String>> mName2cNames;

    /**
     * Initialize a WorkflowParser
     *
     * @param userId the user id. Currently we have just checked single user
     *               mode
     */
    public WorkflowParser(int userId) {
        this.userId = userId;
        this.mName2Task = new HashMap<>();
        this.mName2pNames = new HashMap<>();
        this.mName2cNames = new HashMap<>();
        this.daxPath = Parameters.getDaxPath();
        this.daxPaths = Parameters.getDAXPaths();
        this.jobIdStartsFrom = 1;

        setTaskList(new ArrayList<>());
    }

    /**
     * Start to parse a workflow which is a xml file(s).
     */
    public void parse() {
        if (this.daxPath != null) {
            parseXmlFile(this.daxPath);
        } else if (this.daxPaths != null) {
            for (String path : this.daxPaths) {
                parseXmlFile(path);
            }
        }
    }

    /**
     * Sets the depth of a task
     *
     * @param task  the task
     * @param depth the depth
     */
    private void setDepth(Task task, int depth) {
        if (depth > task.getDepth()) {
            task.setDepth(depth);
        }
        for (Task cTask : task.getChildList()) {
            setDepth(cTask, task.getDepth() + 1);
        }
    }

    /**
     * Parse a DAX file with jdom
     */
    private void parseXmlFile(String path) {

        try {

            SAXBuilder builder = new SAXBuilder();
            //parse using builder to get DOM representation of the XML file
            Document dom = builder.build(new File(path));
            Element root = dom.getRootElement();
            List<Element> list = root.getChildren();

            //NormalDistribution normalDistribution = new NormalDistribution(1, 0.5);
            //Random random = new Random();

            for (Element node : list) {
                switch (node.getName().toLowerCase()) {
                    case "job":
                        long length = 0;
                        String nodeName = node.getAttributeValue("id");
                        String nodeType = node.getAttributeValue("name");
                        String workflow = node.getAttributeValue("namespace");
                        Integer numcores = 1;
                        if (node.getAttributeValue("numcores") != null) {
                            numcores = Integer.parseInt(node.getAttributeValue("numcores"));
                        }
                        /**
                         * capture runtime. If not exist, by default the runtime
                         * is 0.1. Otherwise CloudSim would ignore this task.
                         * BUG/#11
                         */
                        double runtime;
                        if (node.getAttributeValue("runtime") != null) {
                            String nodeTime = node.getAttributeValue("runtime");
                            runtime = 10000 * Double.parseDouble(nodeTime);
                            if (runtime < 100) {
                                runtime = 100;
                            }
                            length = (long) runtime;
                        } else {
                            Log.printLine("Cannot find runtime for " + nodeName + ",set it to be 0");
                        }   //multiple the scale, by default it is 1.0
                        length *= Parameters.getRuntimeScale();
                        long lengthWithNoise;
                        /**    if (random.nextDouble() > 0.5) {
                         lengthWithNoise = (long) (length * (1 + normalDistribution.sample() * 0.25));
                         } else {
                         lengthWithNoise = (long) (length * (1 - normalDistribution.sample() * 0.25));
                         }
                         **/

                        List<Element> fileList = node.getChildren();
                        List<FileItem> mFileList = new ArrayList<>();
                        for (Element file : fileList) {
                            if (file.getName().toLowerCase().equals("uses")) {
                                String fileName = file.getAttributeValue("name");//DAX version 3.3
                                if (fileName == null) {
                                    fileName = file.getAttributeValue("file");//DAX version 3.0
                                }
                                if (fileName == null) {
                                    Log.print("Error in parsing xml");
                                }

                                String inout = file.getAttributeValue("link");
                                double size = 0.0;

                                String fileSize = file.getAttributeValue("size");
                                if (fileSize != null) {
                                    size = Double.parseDouble(fileSize) /*/ 1024*/;
                                } else {
                                    Log.printLine("File Size not found for " + fileName);
                                }

                                /**
                                 * a bug of cloudsim, size 0 causes a problem. 1
                                 * is ok.
                                 */
                                if (size == 0) {
                                    size++;
                                }
                                /**
                                 * Sets the file type 1 is input 2 is output
                                 */
                                FileType type = FileType.NONE;
                                switch (inout) {
                                    case "input":
                                        type = FileType.INPUT;
                                        break;
                                    case "output":
                                        type = FileType.OUTPUT;
                                        break;
                                    default:
                                        Log.printLine("Parsing Error");
                                        break;
                                }
                                FileItem tFile;
                                /*
                                 * Already exists an input file (forget output file)
                                 */
                                if (size < 0) {
                                    /*
                                     * Assuming it is a parsing error
                                     */
                                    size = 0 - size;
                                    Log.printLine("Size is negative, I assume it is a parser error");
                                }
                                /*
                                 * Note that CloudSim use size as MB, in this case we use it as Byte
                                 */
                                if (type == FileType.OUTPUT) {
                                    /**
                                     * It is good that CloudSim does tell
                                     * whether a size is zero
                                     */
                                    tFile = new FileItem(fileName, size);
                                } else if (ReplicaCatalog.containsFile(fileName)) {
                                    tFile = ReplicaCatalog.getFile(fileName);
                                } else {

                                    tFile = new FileItem(fileName, size);
                                    ReplicaCatalog.setFile(fileName, tFile);
                                }

                                tFile.setType(type);
                                mFileList.add(tFile);

                            }
                        }
                        Task task;
                        //In case of multiple workflow submission. Make sure the jobIdStartsFrom is consistent.
                        synchronized (this) {
                            task = new Task(this.jobIdStartsFrom, length);
                            this.jobIdStartsFrom++;
                        }
                        task.setType(nodeType);
                        task.setWorkflow(workflow);
                        task.setUserId(userId);
                        task.setNumberOfPes(numcores);
                        // task.setCloudletLengthWithNoise(lengthWithNoise);
                        mName2Task.put(nodeName, task);
                        mName2pNames.put(nodeName, new ArrayList<>());
                        mName2cNames.put(nodeName, new ArrayList<>());
                        for (FileItem file : mFileList) {
                            task.addRequiredFile(file.getName());
                        }
                        task.setFileList(mFileList);
                        this.getTaskList().add(task);
                        /**
                         * Add dependencies info.
                         */
                        break;
                    case "child":
                        List<Element> pList = node.getChildren();
                        String childName = node.getAttributeValue("ref");
                        if (mName2Task.containsKey(childName)) {

                            Task childTask = (Task) mName2Task.get(childName);

                            for (Element parent : pList) {
                                String parentName = parent.getAttributeValue("ref");
                                if (mName2Task.containsKey(parentName)) {
                                    mName2pNames.get(childName).add(parentName);
                                    mName2cNames.get(parentName).add(childName);

                                    Task parentTask = (Task) mName2Task.get(parentName);
                                    parentTask.addChild(childTask);
                                    childTask.addParent(parentTask);
                                }
                            }
                        }
                        break;
                }
            }

            // set depths of tasks
            int cLevel = 0;
            ArrayList<String> doneNames = new ArrayList<>();
            ArrayList<String> remainingNames = new ArrayList<>(mName2Task.keySet());

            // find root tasks and mark them done
            for (String mName : remainingNames) {
                if (mName2pNames.get(mName).size() == 0) {
                    doneNames.add(mName);
                    mName2Task.get(mName).setDepth(cLevel);
                }
            }
            remainingNames.removeAll(doneNames);
            cLevel += 1;

            while (remainingNames.size() > 0) {
                for (String mName : remainingNames) {
                    // check if any parent is marked done
                    for (String pName : mName2pNames.get(mName)) {
                        if (doneNames.contains(pName)) {
                            doneNames.add(mName);
                            mName2Task.get(mName).setDepth(cLevel);
                            break;
                        }
                    }
                }
                remainingNames.removeAll(doneNames);
                // increment level
                cLevel += 1;
            }

            // set impacts of tasks
            doneNames = new ArrayList<>();
            remainingNames = new ArrayList<>(mName2Task.keySet());
            // find exit tasks and mark them done
            ArrayList<String> exits = new ArrayList<>();
            for (String mName : mName2Task.keySet()) {
                if (mName2cNames.get(mName).size() == 0) {
                    exits.add(mName);
                }
            }
            double seedImpact = 1.0 / exits.size();
            for (String mName : exits) {
                mName2Task.get(mName).setImpact(seedImpact);
                doneNames.add(mName);
                remainingNames.remove(mName);
            }

            while (remainingNames.size() > 0) {
                for (String mName : remainingNames) {
                    if (doneNames.containsAll(mName2cNames.get(mName))) {
                        double newImpact = 0;
                        for (String cName : mName2cNames.get(mName)) {
                            Task cTask = mName2Task.get(cName);
                            newImpact += cTask.getImpact() / cTask.getParentList().size();
                        }
                        mName2Task.get(mName).setImpact(newImpact);
                        //
                        doneNames.add(mName);
                    }
                }
                remainingNames.removeAll(doneNames);
            }

            /**
             * Clean them to save memory. Parsing workflow may take much memory
             */
            this.mName2Task.clear();
            this.mName2pNames.clear();
            this.mName2cNames.clear();

        } catch (JDOMException jde) {
            Log.printLine("JDOM Exception;Please make sure your dax file is valid");

        } catch (IOException ioe) {
            Log.printLine("IO Exception;Please make sure dax.path is correctly set in your config file");

        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Parsing Exception");
        }
    }
}
