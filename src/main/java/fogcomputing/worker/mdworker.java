package fogcomputing.worker;

import fogcomputing.titanic.mdwrkapi;
import org.zeromq.ZMsg;

public class mdworker
{
    public static void main(String[] args)
    {
        boolean verbose = (args.length > 0 && "-v".equals(args[0]));
        mdwrkapi workerSession = new mdwrkapi("tcp://localhost:5555", "echo", verbose);

        ZMsg reply = null;
        while (!Thread.currentThread().isInterrupted()) {
            ZMsg request = workerSession.receive(reply);
            if (request == null)
                break;
            reply = request;
        }
        workerSession.destroy();
    }
}

