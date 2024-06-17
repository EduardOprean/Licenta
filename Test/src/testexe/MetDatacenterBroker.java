package testexe;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

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
        int vmIndex = 0;

        for (Cloudlet cloudlet : getCloudletList()) {
            Vm vm = getVmList().get(vmIndex);
            bindCloudletToVm(cloudlet.getCloudletId(), vm.getId());
            cloudletsSubmitted++;
            Integer datacenterId = getVmsToDatacentersMap().get(vm.getId());
            if (datacenterId != null) {
                sendNow(datacenterId, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
                getCloudletSubmittedList().add(cloudlet);
                System.out.println(CloudSim.clock() + ": " + getName() + ": Sending cloudlet " + cloudlet.getCloudletId() + " to VM #" + vm.getId());
            } else {
                System.out.println("VM " + vm.getId() + " is not mapped to any datacenter.");
            }
            vmIndex = (vmIndex + 1) % getVmList().size();
        }

        getCloudletList().clear();
    }
}
