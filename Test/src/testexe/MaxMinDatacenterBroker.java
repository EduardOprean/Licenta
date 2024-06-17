package testexe;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;

import java.util.List;

public class MaxMinDatacenterBroker extends DatacenterBroker {

    public MaxMinDatacenterBroker(String name) throws Exception {
        super(name);
    }

    @Override
    protected void submitCloudlets() {
        int vmIndex = 0;
        for (Cloudlet cloudlet : getCloudletList()) {
            Vm vm = getVmsCreatedList().get(vmIndex);
            if (vm.getNumberOfPes() >= cloudlet.getNumberOfPes()) {
                bindCloudletToVm(cloudlet.getCloudletId(), vm.getId());
                vmIndex = (vmIndex + 1) % getVmsCreatedList().size();
            } else {
                Log.printLine("Cloudlet " + cloudlet.getCloudletId() + " requires more PEs than VM " + vm.getId() + " has.");
            }
        }

        for (Cloudlet cloudlet : getCloudletList()) {
            Integer datacenterId = getVmsToDatacentersMap().get(cloudlet.getVmId());
            if (datacenterId != null) {
                sendNow(datacenterId, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
                cloudletsSubmitted++;
            } else {
                Log.printLine("No datacenter mapping found for VM " + cloudlet.getVmId());
            }
        }

        getCloudletList().clear();
    }
}
