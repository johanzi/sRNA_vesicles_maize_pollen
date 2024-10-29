package generatedatasetjoinshellscript;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class GenerateDatasetJoinShellScript {
    public GenerateDatasetJoinShellScript(String fileEnding, String directory) {
        this.fileEnding = fileEnding;
        this.directory = directory;
    }
    
    public void generateScript(String separator) throws IOException {
        ArrayList<String> nextDatasetFileNameArrayList = new ArrayList();
        ArrayList<Integer> nextDatasetColumnsArrayList = new ArrayList();

        StringBuilder headerStringBuilder = new StringBuilder("sequence");
        
        File folder = new File(directory);
        File[] files = folder.listFiles();
        Arrays.sort(files);
        
        for(File file : files) {
            if(!file.getName().endsWith(fileEnding)) {
                continue;
            }
            
            String experimentTitle = file.getName().split(separator)[0];
            headerStringBuilder.append(" " + experimentTitle);

            nextDatasetFileNameArrayList.add(file.getName());
            nextDatasetColumnsArrayList.add(1);
        }
        
        BufferedWriter headerBufferedWriter = new BufferedWriter(new FileWriter(directory + "header.csv"));
        headerBufferedWriter.append(headerStringBuilder.toString());
        headerBufferedWriter.newLine();
        headerBufferedWriter.flush();
        headerBufferedWriter.close();
        
        BufferedWriter scriptBufferedWriter = new BufferedWriter(new FileWriter("merge_datasets.sh"));

        String mergeDirectory = directory;
        int tempIndex = 0;
        do {
            ArrayList<Integer> datasetColumnsArrayList = (ArrayList<Integer>) nextDatasetColumnsArrayList.clone();
            nextDatasetColumnsArrayList = new ArrayList();
            ArrayList<String> datasetFileNameArrayList = (ArrayList<String>) nextDatasetFileNameArrayList;
            nextDatasetFileNameArrayList = new ArrayList();
            
            for(int mergeIndex = 0; mergeIndex < (datasetColumnsArrayList.size() / 2); mergeIndex++) {
                nextDatasetColumnsArrayList.add(datasetColumnsArrayList.get(mergeIndex * 2 + 0) + datasetColumnsArrayList.get(mergeIndex * 2 + 1));
                
                scriptBufferedWriter.append("join -a1 -a2 -e'0' -o'0");
                for(int datasetIndex = 0; datasetIndex <= 1; datasetIndex++) {
                    for(int datasetColumnIndex = 1; datasetColumnIndex <= datasetColumnsArrayList.get(mergeIndex * 2 + datasetIndex); datasetColumnIndex++) {
                        scriptBufferedWriter.append(" " + (datasetIndex + 1) + "." + (datasetColumnIndex + 1));
                    }
                }
                scriptBufferedWriter.append("' " + mergeDirectory + datasetFileNameArrayList.get(mergeIndex * 2 + 0) + " " + mergeDirectory + datasetFileNameArrayList.get(mergeIndex * 2 + 1) + " > temp" + tempIndex);
                scriptBufferedWriter.newLine();
                
                nextDatasetFileNameArrayList.add("temp" + tempIndex);
                tempIndex++;
            }
            
            if((datasetColumnsArrayList.size() % 2) == 1) {
                nextDatasetColumnsArrayList.add(datasetColumnsArrayList.get(datasetColumnsArrayList.size() - 1));
                nextDatasetFileNameArrayList.add(mergeDirectory + datasetFileNameArrayList.get(datasetFileNameArrayList.size() - 1));
            }
            
            scriptBufferedWriter.newLine();
            scriptBufferedWriter.flush();
            
            mergeDirectory = "";
        } while(nextDatasetColumnsArrayList.size() > 1);
        
        scriptBufferedWriter.append("mv temp" + (tempIndex - 1) + " " + directory + "merged_dataset.csv");
        scriptBufferedWriter.newLine();
        scriptBufferedWriter.append("cat " + directory + "header.csv " + directory + "merged_dataset.csv > " + directory + "merged_dataset_with_header.csv");
        scriptBufferedWriter.newLine();
        scriptBufferedWriter.append("rm -f temp*");
        scriptBufferedWriter.newLine();
        scriptBufferedWriter.flush();
        
        scriptBufferedWriter.close();
    }
    
    public static void main(String[] args) {
        String fileEnding = null;
        String directory = null;
        String separator = "_";
        
        if(args.length == 0) {
            System.out.println("-fileEnding <extension>");
            System.out.println("-directory <directory>\trelative path to input directory");
            System.out.println("-separator <string>\tseparator from sample name and description in filename (default: _)");
            System.exit(1);
        }
        else {
            for(int argumentIndex = 0; argumentIndex < args.length; argumentIndex++) {
                if(args[argumentIndex].startsWith("-")) {
                    String argumentTitle = args[argumentIndex].substring(1);
                    argumentIndex++;
                    String argumentValue = args[argumentIndex];
                    
                    if(argumentTitle.equals("fileEnding")) {
                        fileEnding = argumentValue;
                    }
                    else if(argumentTitle.equals("directory")) {
                        directory = argumentValue;
                    }
                    else if(argumentTitle.equals("separator")) {
                        separator = argumentValue;
                    }
                }
            }
        }

        if((fileEnding != null)  && (directory != null)) {
            try {
                GenerateDatasetJoinShellScript generateDatasetJoinShellScript = new GenerateDatasetJoinShellScript(fileEnding, directory);
                generateDatasetJoinShellScript.generateScript(separator);
            }
            catch(IOException e) {
                System.out.println(e);
            }
        }
        else {
            System.out.println("-fileEnding <extension>");
            System.out.println("-directory <directory>\trelative path to input directory");
            System.out.println("-separator <string>\tseparator from sample name and description in filename (default: _)");
            System.exit(1);
        }
    }
    
    private String fileEnding;
    private String directory;
}
