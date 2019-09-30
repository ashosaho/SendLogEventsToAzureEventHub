package com.phdata.sample;

//Import statements
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import org.json.simple.JSONObject;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SendLogEvent {

    //Main method
    public static void main(String[] args)
            throws EventHubException, ExecutionException, InterruptedException, IOException, Exception {

        // Creating a regular expression for the records
        final String regex = "^(\\S+) (\\S+) (\\S+) " +
                "\\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+)" +
                " (\\S+)\\s*(\\S+)?\\s*\" (\\d{3}) (\\S+)";
        final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);

        //Date Format
        SimpleDateFormat formatter=new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");

        //Connection String to the Azure EventHub
        final ConnectionStringBuilder connStr = new ConnectionStringBuilder()
                .setNamespaceName("Namespace Name")
                .setEventHubName("Event Hub Name")
                .setSasKeyName("SAS Key Name")
                .setSasKey("Primary SAS Key");

        //Gson Builder
        final Gson gson = new GsonBuilder().create();

        /*
        The Executor handles all asynchronous tasks and this is passed to the EventHubClient instance.
        This enables the user to segregate their thread pool based on the work load.
        This pool can then be shared across multiple EventHubClient instances.
        The following sample uses a single thread executor, as there is only one EventHubClient instance,
        handling different flavors of ingestion to Event Hubs here.
         */
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);

        /*
        Each EventHubClient instance spins up a new TCP/SSL connection, which is expensive.
        It is always a best practice to reuse these instances. The following sample shows this.
         */
        final EventHubClient ehClient = EventHubClient.createFromConnectionStringSync(connStr.toString(), executorService);

        try {
                //Create a new file Input Stream
                FileInputStream fstream = new FileInputStream("path to log file");

                //Create a new BufferedReader from File Input Stream Reader
                BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

                //Temporary Variable to store one line entry in the file
                String line;

                //Infinite loop to keep on reading the log file
                while (true) {

                    //Read the line
                    line = br.readLine();

                    //Sleep if reached EOF
                    if (line == null) {
                        Thread.sleep(500);
                    } else {
                        //Perform Pattern Matching for log line
                        final Matcher matcher = pattern.matcher(line);

                        while (matcher.find()){

                            //Fetch the information from the log line
                            String remoteip = matcher.group(1);
                            String identd = matcher.group(2);
                            String userid  = matcher.group(3);
                            Date time =formatter.parse(matcher.group(4).split(" ")[0]
                                    .replaceFirst(":"," ")
                                    .replaceAll("/","-"));
                            String method = matcher.group(5);
                            String path =  matcher.group(6);
                            String protocol   = matcher.group(7);
                            BigInteger status = new BigInteger(matcher.group(8));
                            BigInteger size = new BigInteger(matcher.group(9));

                            //Built the JSON Payload
                            JSONObject payload = new JSONObject();
                            payload.put("remoteip", remoteip);
                            payload.put("identd", identd);
                            payload.put("userid", userid);
                            payload.put("time", time);
                            payload.put("method", method);
                            payload.put("path", path);
                            payload.put("protocol", protocol);
                            payload.put("status", status);
                            payload.put("size", size);

                            //Convert payload to bytes for wire transfer
                            byte[] payloadBytes = gson.toJson(payload).getBytes(Charset.defaultCharset());
                            EventData sendEvent = EventData.create(payloadBytes);

                            /* Send - not tied to any partition
                            Event Hubs service will round-robin the events across all Event Hubs partitions.
                            This is the recommended & most reliable way to send to Event Hubs.
                             */
                            //Send the payload to EventHub
                            ehClient.sendSync(sendEvent);
                        }
                    }
                }
        } finally {
            //Release the Services
            ehClient.closeSync();
            executorService.shutdown();
        }
    }
}
