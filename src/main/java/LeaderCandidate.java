import dcf.ConsensusApplication;
import dcf.DistributedConsensusFramework;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderCandidate extends ConsensusApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderCandidate.class);

    public LeaderCandidate(String nodeId, String runtimeJsCode, String evaluationJsCode, String kafkaServerAddress,
                           String kafkaTopic) {
        super(nodeId, runtimeJsCode, evaluationJsCode, kafkaServerAddress, kafkaTopic);
    }

    @Override
    public Boolean onReceiving(Value result) {
        Boolean consensusResult = result.getMember("consensus").asBoolean();
        Value agreedValue = result.getMember("value");
        if (consensusResult){
            System.out.println(agreedValue);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        else{
            System.out.println(false);
        }
        return consensusResult;
    }

    public static void electLeader(String clientId, int instanceCount, String kafkaServer){

        final LeaderCandidate leaderCandidate = new LeaderCandidate(clientId, "var clientRanks = [];" +
                "result = {consensus:false, value:null};",
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

        final DistributedConsensusFramework dcf = new DistributedConsensusFramework(leaderCandidate.getNodeId(),
                leaderCandidate.getRuntimeJsCode(), leaderCandidate.getEvaluationJsCode(), kafkaServer, leaderCandidate
                .getKafkaTopic(), leaderCandidate);

        System.out.println(leaderCandidate.getNodeId());

        int clientRank = (int)(1 + Math.random()*100);
        dcf.writeACommand("clientRanks.push({client:\""+ leaderCandidate.getNodeId() + "\",rank:" +
                clientRank +"});");
        System.out.println(clientRank);
    }

    public static void main(String args[]){
        LeaderCandidate.electLeader(args[0], Integer.parseInt(args[1]),"localhost:9092");
    }
}
