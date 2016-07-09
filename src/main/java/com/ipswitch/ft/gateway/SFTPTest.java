package com.ipswitch.ft.gateway;

import com.jcraft.jsch.*;
import org.apache.commons.cli.*;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.future.ConnectFuture;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.client.subsystem.sftp.SftpClient;

import java.io.*;
import java.util.Arrays;

/**
 * Uses Apache SSHD client to upload files.
 * This is for a performance test of the SFTP client currently used in MINA Gate.
 *
 * Created by mriordan on 6/23/16.
 */
public class SFTPTest {

    ProgramArgs programArgs;
    public enum Client {CLIENT_SSHD, CLIENT_JSCH};

    private class ProgramArgs {
        private String hostname;
        private int port;
        private String username;
        private String password;
        private String localDir;
        private String remoteDir;
        private Client client;

        public String getHostname() {
            return hostname;
        }

        public void setHostname(String hostname) {
            this.hostname = hostname;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getLocalDir() {
            return localDir;
        }

        public void setLocalDir(String localDir) {
            this.localDir = localDir;
        }

        public String getRemoteDir() {
            return remoteDir;
        }

        public void setRemoteDir(String remoteDir) {
            this.remoteDir = remoteDir;
        }

        public Client getClient() {
            return client;
        }

        public void setClient(String parmClient) {
            if(parmClient.equals("sshd")) {
                client = Client.CLIENT_SSHD;
            } else if(parmClient.equals("jsch")) {
                client = Client.CLIENT_JSCH;
            } else {
                throw new IllegalArgumentException("Invalid client type");
            }
        }

        @Override
        public String toString() {
            return "ProgramArgs{" +
                    "hostname='" + hostname + '\'' +
                    ", port=" + port +
                    ", username='" + username + '\'' +
                    ", password='" + password + '\'' +
                    ", localDir='" + localDir + '\'' +
                    ", remoteDir='" + remoteDir + '\'' +
                    ", client=" + client +
                    '}';
        }
    }

    private ProgramArgs parseArgs(String [] args) throws Exception {
        // Get arguments from command line.
        programArgs = new ProgramArgs();

        CommandLineParser clp = new DefaultParser();
        Options options = new Options()
                .addOption(
                        Option.builder()
                                .longOpt("host")
                                .argName("hostname or IP address to which we should connect")
                                .required()
                                .hasArg()
                                .valueSeparator()
                                .build()
                )
                .addOption(
                        Option.builder()
                                .longOpt("port")
                                .argName("port to which we should connect")
                                .hasArg()
                                .valueSeparator()
                                .type(Integer.class)
                                .build()
                )
                .addOption(
                        Option.builder()
                                .longOpt("user")
                                .argName("username")
                                .required()
                                .hasArg()
                                .valueSeparator()
                                .build()
                )
                .addOption(
                        Option.builder()
                                .longOpt("password")
                                .argName("password for the user")
                                .required()
                                .hasArg()
                                .valueSeparator()
                                .build()
                )
                .addOption(
                        Option.builder()
                                .longOpt("localdir")
                                .argName("local directory from which we upload all files")
                                .required()
                                .hasArg()
                                .valueSeparator()
                                .build()
                )
                .addOption(
                        Option.builder()
                                .longOpt("remotedir")
                                .argName("remote directory to which we upload files")
                                .hasArg()
                                .valueSeparator()
                                .build()
                )
                .addOption(
                        Option.builder()
                                .longOpt("client")
                                .argName("client library to use:  sshd or jsch")
                                .required()
                                .hasArg()
                                .valueSeparator()
                                .build()
                )
                ;

        CommandLine commandLine = clp.parse(options, args, true);
        programArgs.setHostname(commandLine.getOptionValue("host"));
        programArgs.setPort(Integer.parseInt(commandLine.getOptionValue("port", "22")));
        programArgs.setUsername(commandLine.getOptionValue("user"));
        programArgs.setPassword(commandLine.getOptionValue("password"));
        programArgs.setLocalDir(commandLine.getOptionValue("localdir"));
        programArgs.setRemoteDir(commandLine.getOptionValue("remotedir"));
        programArgs.setClient(commandLine.getOptionValue("client"));

        return programArgs;
    }

    private abstract class SFTPClient {
        public abstract void connect(ProgramArgs programArgs) throws Exception;
        //public abstract void openWrite(String remotePath) throws Exception;
        public abstract void sendFile(InputStream inputStream, String remotePath) throws Exception;
        //public abstract void writeBuf(byte[] buf, int nBytes, long offset) throws Exception;
        //public abstract void closeFile() throws Exception;
        public abstract void disconnnect() throws Exception;

        public void doUploadFile(File localFile) throws Exception {
            DataInputStream inputStream = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(localFile)));

            long totalBytes = localFile.length();

            String remotePath = programArgs.getRemoteDir();
            if(remotePath.length() > 0) remotePath += "/";
            remotePath += localFile.getName();
            System.out.println("Creating " + remotePath + " on SFTP server");

            long startMillis = System.currentTimeMillis();

            sendFile(inputStream, remotePath);

            long elapsedMillis = System.currentTimeMillis() - startMillis;
            double secs = 0.001 * elapsedMillis;
            double kbPerSec = (totalBytes / secs) / 1024.0 ;
            System.out.println("Closing handle.  Sent " + totalBytes + " bytes in "
                    + String.format("%.3f", secs) + " secs at " + String.format("%.2f", kbPerSec) + " KB/sec"
                    );
            //closeFile();
        }
    }

    private class SSHDSFTPClient extends SFTPClient {
        private SshClient client;
        private SftpClient sftp;
        SftpClient.CloseableHandle fileHandle;

        public void connect(ProgramArgs programArgs) throws Exception {
            client = SshClient.setUpDefaultClient();
            client.start();
            System.out.println("Connecting...");

            ConnectFuture future =  client.connect(programArgs.getUsername(),
                    programArgs.getHostname(), programArgs.getPort());
            System.out.println("Awaiting...");
            boolean success = future.await(4000);
            System.out.println("await returned " + (success ? "true":"false"));
            ClientSession session = future.getSession();
            System.out.println("Got session.  Now to authenticate.");
            session.addPasswordIdentity(programArgs.getPassword());
            session.auth().verify(4000);
            System.out.println("Logged in");

            sftp  =  session.createSftpClient();
        }

        public void openWrite(String remotePath) throws Exception {
            fileHandle = sftp.open(remotePath, SftpClient.OpenMode.Write);
        }

        public void writeBuf(byte [] buf, int nBytes, long offset) throws Exception {
            sftp.write(fileHandle, offset, buf, 0, nBytes);
        }

        public void sendFile(InputStream inputStream, String remotePath) throws Exception {
            openWrite(remotePath);
            byte [] inbuf = new byte[32768];
            long destOffset = 0;
            long totalBytes = 0;
            int nWrites = 0;
            do {
                int bytesRead = inputStream.read(inbuf);
                if(bytesRead <= 0) break;
                writeBuf(inbuf, bytesRead, destOffset);
                nWrites++;
                destOffset += bytesRead;
                totalBytes += bytesRead;
            } while(true);
            closeFile();
        }

        public void closeFile() throws Exception {
            fileHandle.close();
        }

        public void disconnnect() {
            client.stop();
        }
    }


    private class JschSFTPClient extends SFTPClient {
        Session session;
        ChannelSftp channelSftp;

        public void connect(ProgramArgs programArgs) throws Exception {
            JSch jsch=new JSch();

            session=jsch.getSession(programArgs.getUsername(),
                    programArgs.getHostname(), programArgs.getPort());
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(programArgs.getPassword());
            session.connect();

            Channel channel=session.openChannel("sftp");
            channel.connect();
            channelSftp = (ChannelSftp)channel;
        }

        public void sendFile(InputStream inputStream, String remotePath) throws Exception {
            channelSftp.put(inputStream, remotePath);
        }

        public void disconnnect() {
            session.disconnect();
        }
    }


    private SFTPClient sftpClient;

    private void loopThruFiles() throws Exception {
        File dir = new File(programArgs.getLocalDir());
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
            for (File localFile : directoryListing) {
                sftpClient.doUploadFile(localFile);
            }
        }
    }

    public void doSFTP(String [] args) throws Exception {
        programArgs = parseArgs(args);
        if(programArgs.getClient() == Client.CLIENT_SSHD) {
            sftpClient = new SSHDSFTPClient();
        } else if(programArgs.getClient() == Client.CLIENT_JSCH) {
            sftpClient = new JschSFTPClient();
        } else {
            throw new IllegalArgumentException("Unexpected SFTP client type");
        }
        sftpClient.connect(programArgs);

        loopThruFiles();

        sftpClient.disconnnect();
    }

    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        SFTPTest sftpTest = new SFTPTest();
        sftpTest.doSFTP(args);
    }
}
