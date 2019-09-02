import dcf.ConsensusApplication;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderElector extends ConsensusApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderElector.class);

    public LeaderElector(String nodeId, String initialJsCode, String evaluationJsCode, String kafkaServerAddress, String kafkaTopic) {
        super(nodeId, initialJsCode, evaluationJsCode, kafkaServerAddress, kafkaTopic);
    }

    public void processACommand() {

        org.graalvm.polyglot.Context jsContext = Context.create("js");

        try {
            while (!this.getConsensusAchieved()) {
                ConsumerRecords<String, String> records = this.getKafkaConsumer().poll(10);

                for (ConsumerRecord<String, String> record : records) {
                    if (record.value().equals("RESET")){
                        this.setInitialJsCode("var clientRanks = [];" + "result = {consensus:false, value:null};");
                    }
                    else {
                        this.setInitialJsCode(this.getInitialJsCode() + record.value());
                        Value result = jsContext.eval("js", this.getInitialJsCode() + this.getEvaluationJsCode());

                        Boolean consensusResult = ((Value) result).getMember("consensus").asBoolean();
                        Value agreedValue = result.getMember("value");

                        if (consensusResult) {
                            this.writeACommand("RESET");
                            this.setConsensusAchieved(true);
                            System.out.println(agreedValue);
                            Thread.sleep(20000);
                        } else {
                            System.out.println(false);
                        }
                    }
                }
            }

        } catch(Exception exception) {
            LOGGER.error("Exception occurred while processing command", exception);
        }finally {
            this.getKafkaConsumer().close();
        }
    }

    public static void electLeader(String clientId, int instanceCount, String kafkaServer){
        final LeaderElector clientInstance = new LeaderElector(clientId, "var clientRanks = [];" +
                "result = {consensus:false, value:\"null\"};",
                "if(Object.keys(clientRanks).length==" + instanceCount + "){" +
                        "var leader = null;"+
                        "var maxRank = 0;"+
                        "for (var i = 0; i < clientRanks.length; i++) {"+
                        "if(clientRanks[i].rank > maxRank){"+
                        "result.consensus=true;" +
                        "result.value = clientRanks[i].client;" +
                        "maxRank = clientRanks[i].rank;" +
                        "}" +
                        "}" +
                        "}" +
                        "result;",
                kafkaServer, "Leader");

        System.out.println(clientInstance.getNodeId());


        Runnable consuming = new Runnable() {
            @Override
            public void run() {
                clientInstance.processACommand();
            }
        };
        new Thread(consuming).start();

        int clientRank = (int)(1 + Math.random()*100);
        clientInstance.writeACommand("clientRanks.push({client:\""+ clientInstance.getNodeId() + "\",rank:" +
                clientRank +"});");
        System.out.println(clientRank);
    }

    public static void main(String args[]){
        LeaderElector.electLeader(args[0], Integer.parseInt(args[1]),"localhost:9092");
    }
}
