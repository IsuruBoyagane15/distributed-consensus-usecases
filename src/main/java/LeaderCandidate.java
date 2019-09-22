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

    public static void electLeader(String nodeId, int instanceCount, String kafkaServerAddress, String kafkaTopic){

        LeaderCandidate leaderCandidate = new LeaderCandidate(nodeId, "var nodeRanks = [];result = {consensus:false, value:null};",
"if(Object.keys(nodeRanks).length==" + instanceCount + "){" +
                    "result.consensus=true;" +
                    "var leader = null;"+
                    "var maxRank = 0;"+
                    "for (var i = 0; i < nodeRanks.length; i++) {"+
                        "if(nodeRanks[i].rank > maxRank){"+
                            "result.value = nodeRanks[i].client;" +
                            "maxRank = nodeRanks[i].rank;" +
                        "}" +
                    "}" +
                "}" +
                "result;",
                kafkaServerAddress, kafkaTopic);

        DistributedConsensusFramework consensusFramework = new DistributedConsensusFramework(leaderCandidate);
        consensusFramework.start();

        System.out.println(leaderCandidate.getNodeId());

        int nodeRank = (int)(1 + Math.random()*100);
        System.out.println(nodeRank);
        consensusFramework.writeACommand("nodeRanks.push({client:\""+ nodeId + "\",rank:" +
                nodeRank +"});");
    }

    public static void main(String args[]){
        LeaderCandidate.electLeader(args[0], Integer.parseInt(args[1]),args[2], args[3]);
    }
}
