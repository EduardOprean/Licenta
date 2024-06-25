package testexe;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

import java.util.ArrayList;
import java.util.List;

public class MetDatacenterBroker extends DatacenterBroker {

    public MetDatacenterBroker(String name) throws Exception {
        super(name);
    }

    @Override
    protected void processCloudletReturn(SimEvent ev) {
        Cloudlet cloudlet = (Cloudlet) ev.getData();
        getCloudletReceivedList().add(cloudlet);
        System.out.println(CloudSim.clock() + ": " + getName() + ": Cloudlet " + cloudlet.getCloudletId() + " received");
        cloudletsSubmitted--;
        if (getCloudletList().size() == 0 && cloudletsSubmitted == 0) {
            clearDatacenters();
            finishExecution();
        } else if (getCloudletList().size() > 0 && cloudletsSubmitted == 0) {
            clearDatacenters();
            createVmsInDatacenter(0);
        }
    }

    @Override
    protected void submitCloudlets() {
        List<Cloudlet> cloudletList = new ArrayList<>(getCloudletList());
        List<Vm> vmList = getVmsCreatedList();

        while (!cloudletList.isEmpty()) {
            Cloudlet minCloudlet = null;
            Vm selectedVm = null;
            double minExecTime = Double.MAX_VALUE;

            // Find the cloudlet with the minimum execution time
            for (Cloudlet cloudlet : cloudletList) {
                for (Vm vm : vmList) {
                    double execTime = cloudlet.getCloudletLength() / vm.getMips();
                    if (execTime < minExecTime) {
                        minExecTime = execTime;
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
                    System.out.println(CloudSim.clock() + ": " + getName() + ": Sending cloudlet " + minCloudlet.getCloudletId() + " to VM #" + selectedVm.getId());
                } else {
                    System.out.println("VM " + selectedVm.getId() + " is not mapped to any datacenter.");
                }
                cloudletList.remove(minCloudlet);
            } else {
                System.out.println("Failed to find a suitable VM for cloudlet scheduling.");
                break;
            }
        }

        getCloudletList().clear();
    }
}
