package testexe;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.DatacenterBroker;

import java.util.List;

public class RRDatacenterBroker extends DatacenterBroker {

    private int currentVmIndex;

    public RRDatacenterBroker(String name) throws Exception {
        super(name);
        currentVmIndex = 0;
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
        List<Cloudlet> cloudletList = getCloudletList();
        List<Vm> vmList = getVmsCreatedList();

        for (Cloudlet cloudlet : cloudletList) {
            Vm vm = vmList.get(currentVmIndex);
            bindCloudletToVm(cloudlet.getCloudletId(), vm.getId());
            Integer datacenterId = getVmsToDatacentersMap().get(vm.getId());
            if (datacenterId != null) {
                sendNow(datacenterId, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
                cloudletsSubmitted++;
                System.out.println(CloudSim.clock() + ": " + getName() + ": Sending cloudlet " + cloudlet.getCloudletId() + " to VM #" + vm.getId());
            } else {
                System.out.println("VM " + vm.getId() + " is not mapped to any datacenter.");
            }
            currentVmIndex = (currentVmIndex + 1) % vmList.size();
        }

        cloudletList.clear();
    }
}
