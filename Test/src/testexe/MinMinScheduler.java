package testexe;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

public class MinMinScheduler {

    private static List<Cloudlet> cloudletList;
    private static List<Vm> vmList;
    private static Datacenter[] datacenters;

    public static void main(String[] args) {
        System.out.println("Starting MinMin Scheduler...");

        Properties properties = new Properties();
        try (InputStream input = MinMinScheduler.class.getResourceAsStream("/config.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find config.properties. Using default values.");
            } else {
                properties.load(input);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Set default values if properties are missing
        properties.putIfAbsent("vm_mips", "1000");
        properties.putIfAbsent("vm_pes_number", "1");
        properties.putIfAbsent("vm_ram", "512");
        properties.putIfAbsent("vm_bw", "1000");
        properties.putIfAbsent("vm_size", "10000");

        int vmMips = Integer.parseInt(properties.getProperty("vm_mips"));
        int vmPesNumber = Integer.parseInt(properties.getProperty("vm_pes_number"));
        int vmRam = Integer.parseInt(properties.getProperty("vm_ram"));
        int vmBw = Integer.parseInt(properties.getProperty("vm_bw"));
        int vmSize = Integer.parseInt(properties.getProperty("vm_size"));

        try {
            int numUser = 1; // number of cloud users
            Calendar calendar = Calendar.getInstance();
            boolean traceFlag = false; // trace events

            CloudSim.init(numUser, calendar, traceFlag);

            datacenters = new Datacenter[Constants.NO_OF_DATA_CENTERS];
            for (int i = 0; i < Constants.NO_OF_DATA_CENTERS; i++) {
                datacenters[i] = DatacenterCreator.createDatacenter("Datacenter_" + i);
            }

            MinMinDatacenterBroker broker = createBroker("Broker_0");
            int brokerId = broker.getId();

            vmList = createVM(brokerId, Constants.NO_OF_VMS, vmMips, vmPesNumber, vmRam, vmBw, vmSize);
            cloudletList = readCloudletsFromFile("/cloudlet_properties.txt", brokerId);

            broker.submitVmList(vmList);
            broker.submitCloudletList(cloudletList);

            CloudSim.startSimulation();

            List<Cloudlet> receivedCloudletList = broker.getCloudletReceivedList();
            CloudSim.stopSimulation();

            printCloudletList(receivedCloudletList);

            System.out.println(MinMinScheduler.class.getName() + " finished!");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("The simulation has been terminated due to an unexpected error");
        }
    }

    private static MinMinDatacenterBroker createBroker(String name) throws Exception {
        return new MinMinDatacenterBroker(name);
    }

    private static List<Vm> createVM(int userId, int numVms, int mips, int pesNumber, int ram, long bw, long size) {
        List<Vm> vmList = new ArrayList<>();

        for (int i = 0; i < numVms; i++) {
            Vm vm = new Vm(i, userId, mips, pesNumber, ram, bw, size, "Xen", new CloudletSchedulerSpaceShared());
            vmList.add(vm);
        }

        return vmList;
    }

    private static List<Cloudlet> readCloudletsFromFile(String filename, int userId) {
        List<Cloudlet> cloudletList = new ArrayList<>();

        try (InputStream inputStream = MinMinScheduler.class.getResourceAsStream(filename);
             BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            String line = br.readLine(); // Skip the header line
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                int id = Integer.parseInt(values[0]);
                long length = Long.parseLong(values[1]);
                int pesNumber = Integer.parseInt(values[2]);
                long fileSize = Long.parseLong(values[3]);
                long outputSize = Long.parseLong(values[4]);

                Cloudlet cloudlet = new Cloudlet(id, length, pesNumber, fileSize, outputSize,
                        new UtilizationModelFull(), new UtilizationModelFull(), new UtilizationModelFull());
                cloudlet.setUserId(userId);
                cloudletList.add(cloudlet);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return cloudletList;
    }

    private static void printCloudletList(List<Cloudlet> list) {
        int size = list.size();
        Cloudlet cloudlet;

        String indent = "    ";
        System.out.println();
        System.out.println("========== OUTPUT ==========");
        System.out.println("Cloudlet ID" + indent + "STATUS" + indent + "Data center ID" + indent + "VM ID" + indent + indent + "Time" + indent + "Start Time" + indent + "Finish Time");

        DecimalFormat dft = new DecimalFormat("###.##");
        dft.setMinimumIntegerDigits(2);
        for (int i = 0; i < size; i++) {
            cloudlet = list.get(i);
            System.out.print(indent + cloudlet.getCloudletId() + indent + indent);

            if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
                System.out.print("SUCCESS");

                System.out.println(indent + indent + cloudlet.getResourceId() + indent + indent + cloudlet.getVmId() + indent + indent + indent + cloudlet.getActualCPUTime() + indent + indent + cloudlet.getExecStartTime() + indent + indent + cloudlet.getFinishTime());
            }
        }
    }
}
