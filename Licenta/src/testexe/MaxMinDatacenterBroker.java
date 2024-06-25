package testexe;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;

import java.util.ArrayList;
import java.util.List;

public class MaxMinDatacenterBroker extends DatacenterBroker {

    public MaxMinDatacenterBroker(String name) throws Exception {
        super(name);
    }

    @Override
    protected void submitCloudlets() {
        List<Cloudlet> cloudletList = new ArrayList<>(getCloudletList());
        List<Vm> vmList = getVmsCreatedList();

        while (!cloudletList.isEmpty()) {
            Cloudlet maxCloudlet = null;
            Vm selectedVm = null;
            double maxExecTime = -1;
            double minCompletionTime = Double.MAX_VALUE;

            // Find the cloudlet with the maximum execution time
            for (Cloudlet cloudlet : cloudletList) {
                for (Vm vm : vmList) {
                    double execTime = cloudlet.getCloudletLength() / vm.getMips();
                    double completionTime = execTime + getVmFinishTime(vm);
                    
                    if (execTime > maxExecTime || (execTime == maxExecTime && completionTime < minCompletionTime)) {
                        maxExecTime = execTime;
                        minCompletionTime = completionTime;
                        maxCloudlet = cloudlet;
                        selectedVm = vm;
                    }
                }
            }

            if (maxCloudlet != null && selectedVm != null) {
                bindCloudletToVm(maxCloudlet.getCloudletId(), selectedVm.getId());
                Integer datacenterId = getVmsToDatacentersMap().get(selectedVm.getId());
                if (datacenterId != null) {
                    sendNow(datacenterId, CloudSimTags.CLOUDLET_SUBMIT, maxCloudlet);
                    cloudletsSubmitted++;
                } else {
                    Log.printLine("No datacenter mapping found for VM " + selectedVm.getId());
                }
                cloudletList.remove(maxCloudlet);
            } else {
                Log.printLine("Failed to find a suitable VM for cloudlet scheduling.");
                break;
            }
        }

        getCloudletList().clear();
    }

    private double getVmFinishTime(Vm vm) {
        double finishTime = 0.0;
        for (Cloudlet cloudlet : getCloudletReceivedList()) {
            if (cloudlet.getVmId() == vm.getId()) {
                finishTime = Math.max(finishTime, cloudlet.getFinishTime());
            }
        }
        return finishTime;
    }
}
