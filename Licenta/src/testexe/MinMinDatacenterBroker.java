package testexe;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;

import java.util.ArrayList;
import java.util.List;

public class MinMinDatacenterBroker extends DatacenterBroker {

    public MinMinDatacenterBroker(String name) throws Exception {
        super(name);
    }

    @Override
    protected void submitCloudlets() {
        List<Cloudlet> cloudletList = new ArrayList<>(getCloudletList());
        List<Vm> vmList = getVmsCreatedList();

        while (!cloudletList.isEmpty()) {
            Cloudlet minCloudlet = null;
            Vm selectedVm = null;
            double minExecTime = Double.MAX_VALUE;
            double minCompletionTime = Double.MAX_VALUE;

            // Find the cloudlet with the minimum execution time
            for (Cloudlet cloudlet : cloudletList) {
                for (Vm vm : vmList) {
                    double execTime = cloudlet.getCloudletLength() / vm.getMips();
                    double completionTime = execTime + getVmFinishTime(vm);
                    
                    if (execTime < minExecTime || (execTime == minExecTime && completionTime < minCompletionTime)) {
                        minExecTime = execTime;
                        minCompletionTime = completionTime;
                        minCloudlet = cloudlet;
                        selectedVm = vm;
                    }
                }
            }

            if (minCloudlet != null && selectedVm != null) {
                bindCloudletToVm(minCloudlet.getCloudletId(), selectedVm.getId());
                Integer datacenterId = getVmsToDatacentersMap().get(selectedVm.getId());
                if (datacenterId != null) {
                    sendNow(datacenterId, CloudSimTags.CLOUDLET_SUBMIT, minCloudlet);
                    cloudletsSubmitted++;
                } else {
                    Log.printLine("No datacenter mapping found for VM " + selectedVm.getId());
                }
                cloudletList.remove(minCloudlet);
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
