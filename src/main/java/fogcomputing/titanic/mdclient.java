package fogcomputing.titanic;

import org.zeromq.ZMsg;

public class mdclient
{

    public static void main(String[] args)
    {
        boolean verbose = (args.length > 0 && "-v".equals(args[0]));
        mdcliapi clientSession = new mdcliapi("tcp://localhost:5555", true);

        int count;
        for (count = 0; count < 100; count++) {
            ZMsg request = new ZMsg();
            request.addString("Hello world");
            ZMsg reply = clientSession.send("echo", request);
            if (reply != null)
                reply.destroy();
            else break;
        }

        System.out.printf("%d requests/replies processed\n", count);
        clientSession.destroy();
    }

}

