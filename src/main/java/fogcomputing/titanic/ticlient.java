package fogcomputing.titanic;

import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

public class ticlient
{
    static ZMsg serviceCall(mdcliapi session, String service, ZMsg request)
    {
        ZMsg reply = session.send(service, request);
        if (reply != null) {
            ZFrame status = reply.pop();
            if (status.streq("200")) {
                status.destroy();
                return reply;
            }
            else if (status.streq("400")) {
                System.out.println("E: client fatal error, aborting");
            }
            else if (status.streq("500")) {
                System.out.println("E: server fatal error, aborting");
            }
            reply.destroy();
        }
        return null;
    }

    public static void main(String[] args) throws Exception
    {
        boolean verbose = (args.length > 0 && args[0].equals("-v"));
        mdcliapi session = new mdcliapi("tcp://localhost:5555", verbose);


        ZMsg request = new ZMsg();
        request.add("echo");
        request.add("Hello world");
        ZMsg reply = serviceCall(session, "titanic.request", request);

        ZFrame uuid = null;
        if (reply != null) {
            uuid = reply.pop();
            reply.destroy();
            uuid.print("I: request UUID ");
        }

        while (!Thread.currentThread().isInterrupted()) {
            Thread.sleep(100);
            request = new ZMsg();
            request.add(uuid.duplicate());
            reply = serviceCall(session, "titanic.reply", request);

            if (reply != null) {
                String replyString = reply.getLast().toString();
                System.out.printf("Reply: %s\n", replyString);
                reply.destroy();

                request = new ZMsg();
                request.add(uuid.duplicate());
                reply = serviceCall(session, "titanic.close", request);
                reply.destroy();
                break;
            }
            else {
                System.out.println("I: no reply yet, trying again…");
                Thread.sleep(5000); //  Try again in 5 seconds
            }
        }
        uuid.destroy();
        session.destroy();
    }
}
