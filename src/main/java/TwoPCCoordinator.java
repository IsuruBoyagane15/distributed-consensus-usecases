import distributedConsensus.ConsensusApplication;
import distributedConsensus.DistributedConsensus;
import org.graalvm.polyglot.Value;

public class TwoPCCoordinator extends ConsensusApplication {

    private boolean votesCollected;

    public TwoPCCoordinator(String nodeId, String runtimeJsCode, String evaluationJsCode, String kafkaServerAddress, String kafkaTopic) {
        super(nodeId, runtimeJsCode, evaluationJsCode, kafkaServerAddress, kafkaTopic);
        this.votesCollected = false;
    }

    @Override
    public boolean onReceiving(Value value) {
        if (!votesCollected) {
            DistributedConsensus
                    dcf = DistributedConsensus.getDistributeConsensus(this);
            if (value.getMember("votesResult").isNull()){
            }
            else if (value.getMember("votesResult").asBoolean()) {
                dcf.writeACommand("result.commitOrAbort=true;");
                votesCollected = true;
            } else if (!value.getMember("votesResult").asBoolean()) {
                dcf.writeACommand("result.commitOrAbort=false;");
                votesCollected = true;
            }
        }
        return !value.getMember("commitOrAbort").isNull();
    }

    @Override
    public void commitAgreedValue(Value value) {
        DistributedConsensus dcf = DistributedConsensus.getDistributeConsensus(this);
        if (value.getMember("commitOrAbort").asBoolean()){
            System.out.println(getNodeId() + " committed");
        }
        else{
            System.out.println(getNodeId() + " did not commit");
        }
        dcf.setTerminate(true);
    }

    public static  void twoPhaseCommit(String nodeId, String kafkaServerAddress, String kafkaTopic, int instanceCount){
        TwoPCCoordinator twoPCCoordinator = new TwoPCCoordinator(nodeId, "var participantResponses = [];" +
                "result = {coordinatorRequested:null, votesResult:null, commitOrAbort:null};",
                "if (participantResponses.some(response => response.node == \"" + nodeId + "\" && response.vote === true)){" +
                                    "result.coordinatorRequested = true;" +
                                    "if(participantResponses.length == " + instanceCount + "){" +
                                        "if(participantResponses.every(response  => response.vote == true)){" +
                                            "result.votesResult = true;" +
                                        "}" +
                                        "else{" +
                                            "result.votesResult = false;" +
                                        "}" +
                                    "}" +
                                "}" +
                                "result", kafkaServerAddress, kafkaTopic);

        DistributedConsensus
                dcf = DistributedConsensus.getDistributeConsensus(twoPCCoordinator);
        dcf.start();
        dcf.writeACommand("participantResponses.push({node:\"" + nodeId + "\",vote:true});");

    }
    public static void main(String[] args){
        TwoPCCoordinator.twoPhaseCommit(args[0], args[1], args[2], Integer.parseInt(args[3]) );
    }
}
