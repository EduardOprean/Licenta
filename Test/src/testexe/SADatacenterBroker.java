package testexe;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SADatacenterBroker extends DatacenterBroker {

    private List<Vm> vmList;
    private List<Cloudlet> cloudletList;
    private double lastProcessTime;

    public SADatacenterBroker(String name) throws Exception {
        super(name);
        lastProcessTime = 0.0;
    }

 
    protected void processCloudletSubmit(SimEvent ev, boolean ack) {
        updateCloudletProcessing();
        try {
            Cloudlet cl = (Cloudlet) ev.getData();
            cloudletList.add(cl);
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine(getName() + ".processCloudletSubmit(): Error in processing Cloudlet");
        }

        if (ack) {
            int cloudletId = ((Cloudlet) ev.getData()).getCloudletId();
            sendNow(ev.getSource(), CloudSimTags.CLOUDLET_SUBMIT_ACK, cloudletId);
        }
    }

    private void updateCloudletProcessing() {
        double currentTime = CloudSim.clock();
        double timeSpan = currentTime - getLastProcessTime();
        for (Cloudlet cloudlet : getCloudletExecList()) {
            Vm vm = getVmById(cloudlet.getVmId());
            if (vm != null) {
                long length = (long) (cloudlet.getCloudletLength() - (cloudlet.getNumberOfPes() * timeSpan * vm.getMips()));
                if (length < 0) length = 0;
                cloudlet.setCloudletLength(length);
            }
        }
        setLastProcessTime(currentTime);
    }

    private double getLastProcessTime() {
        return lastProcessTime;
    }

    private void setLastProcessTime(double lastProcessTime) {
        this.lastProcessTime = lastProcessTime;
    }

    private List<Cloudlet> getCloudletExecList() {
        List<Cloudlet> execList = new ArrayList<>();
        for (Cloudlet cloudlet : cloudletList) {
            if (cloudlet.getCloudletStatus() == Cloudlet.INEXEC) {
                execList.add(cloudlet);
            }
        }
        return execList;
    }

    private Vm getVmById(int vmId) {
        for (Vm vm : vmList) {
            if (vm.getId() == vmId) {
                return vm;
            }
        }
        return null;
    }

    public void runSimulatedAnnealing() {
        vmList = getVmList();
        cloudletList = getCloudletList();
        double temperature = 10000;
        double coolingRate = 0.003;

        List<Cloudlet> bestSolution = new ArrayList<>(cloudletList);
        double bestCost = calculateCost(bestSolution);

        while (temperature > 1) {
            List<Cloudlet> newSolution = new ArrayList<>(cloudletList);

            Collections.swap(newSolution, (int) (newSolution.size() * Math.random()), (int) (newSolution.size() * Math.random()));

            double currentCost = calculateCost(cloudletList);
            double newCost = calculateCost(newSolution);

            if (acceptanceProbability(currentCost, newCost, temperature) > Math.random()) {
                cloudletList = new ArrayList<>(newSolution);
            }

            if (newCost < bestCost) {
                bestSolution = new ArrayList<>(newSolution);
                bestCost = newCost;
            }

            temperature *= 1 - coolingRate;
        }

        assignCloudletsToVMs(bestSolution);
    }

    private double calculateCost(List<Cloudlet> solution) {
        double cost = 0.0;
        for (Cloudlet cloudlet : solution) {
            Vm vm = getVmById(cloudlet.getVmId());
            if (vm != null) {
                cost += cloudlet.getCloudletLength() / vm.getMips();
            }
        }
        return cost;
    }

    private double acceptanceProbability(double currentCost, double newCost, double temperature) {
        if (newCost < currentCost) {
            return 1.0;
        }
        return Math.exp((currentCost - newCost) / temperature);
    }

    private void assignCloudletsToVMs(List<Cloudlet> solution) {
        for (Cloudlet cloudlet : solution) {
            Vm vm = getNextVm();
            if (vm != null && getVmsToDatacentersMap().get(vm.getId()) != null) {
                cloudlet.setVmId(vm.getId());
                sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
            } else {
                Log.printLine("VM " + vm.getId() + " is not mapped to any datacenter.");
            }
        }
    }

    private Vm getNextVm() {
        if (vmList.isEmpty()) {
            return null;
        }
        Vm vm = vmList.remove(0);
        vmList.add(vm);
        return vm;
    }
}
