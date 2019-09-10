import dcf.ConsensusApplication;
import dcf.DistributedConsensusFramework;
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
    public boolean onReceiving(Value result) {
        return result.getMember("consensus").asBoolean();
    }

    @Override
    public void commitAgreedValue(Value value) {
        System.out.println(value.getMember("value"));
    }

    public static void electLeader(String nodeId, int instanceCount, String kafkaServerAddress){

        LeaderCandidate leaderCandidate = new LeaderCandidate(nodeId, "var clientRanks = [];result = {consensus:false, value:null};",
"if(Object.keys(clientRanks).length==" + instanceCount + "){" +
                    "result.consensus=true;" +
                    "var leader = null;"+
                    "var maxRank = 0;"+
                    "for (var i = 0; i < clientRanks.length; i++) {"+
                        "if(clientRanks[i].rank > maxRank){"+
                            "result.value = clientRanks[i].client;" +
                            "maxRank = clientRanks[i].rank;" +
                        "}" +
                    "}" +
                "}" +
                "result;",
                kafkaServerAddress, "Topic9");

        DistributedConsensusFramework consensusFramework = new DistributedConsensusFramework(leaderCandidate);
        consensusFramework.start();
        System.out.println(leaderCandidate.getNodeId());
        int clientRank = (int)(1 + Math.random()*100);
        System.out.println(clientRank);
        consensusFramework.writeACommand("clientRanks.push({client:\""+ leaderCandidate.getNodeId() + "\",rank:" +
                clientRank +"});");
    }

    public static void main(String args[]){
        LeaderCandidate.electLeader(args[0], Integer.parseInt(args[1]),"localhost:9092");
    }
}
