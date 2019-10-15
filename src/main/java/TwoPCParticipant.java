import distributedConsensus.ConsensusApplication;
import distributedConsensus.DistributedConsensus;
import org.graalvm.polyglot.Value;
import java.util.Random;

public class TwoPCParticipant extends ConsensusApplication {

    private boolean voted;

    public TwoPCParticipant(String nodeId, String runtimeJsCode, String evaluationJsCode, String kafkaServerAddress,
                            String kafkaTopic) {
        super(nodeId, runtimeJsCode, evaluationJsCode, kafkaServerAddress, kafkaTopic);
        voted = false;
    }

    @Override
    public boolean onReceiving(Value value) {
        if (value.getMember("coordinatorRequested").asBoolean()){
            DistributedConsensus
                    dcf = DistributedConsensus.getDistributeConsensus(this);
            //vote may be a false too
            if (!voted){
                Random random = new Random();
                boolean canCommit;
                canCommit = random.nextBoolean();
                dcf.writeACommand("if(!participantResponses.some(response => response.node == \"" + getNodeId()  +
                        "\")){participantResponses.push({node:\""+ getNodeId() + "\",vote:" + canCommit +"});}");
                voted = true;
            }
            return !value.getMember("commitOrAbort").isNull();
        }
        return false;
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

    public static  void twoPhaseCommit(String nodeId, String kafkaServerAddress, String kafkaTopic,
                                       String coordinatorId, int instanceCount){
        TwoPCParticipant twoPCParticipant = new TwoPCParticipant(nodeId, "var participantResponses = [];" +
                                "result = {coordinatorRequested:null, votesResult:null, commitOrAbort:null};",
                "if (participantResponses.some(response => response.node == \"" + coordinatorId + "\" && response.vote === true)){" +
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

        DistributedConsensus dcf = DistributedConsensus.getDistributeConsensus(twoPCParticipant);
        dcf.start();

    }
    public static void main(String[] args){
        TwoPCParticipant.twoPhaseCommit(args[0], args[1], args[2], args[3], Integer.parseInt(args[4]) );
    }
}

