package testexe;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.DatacenterBroker;

import java.util.List;

public class RRDatacenterBroker extends DatacenterBroker {

    private int currentVmIndex;

    public RRDatacenterBroker(String name) throws Exception {
        super(name);
        currentVmIndex = 0;
    }

   
    protected void processCloudletSubmit(SimEvent ev, boolean ack) {
        Cloudlet cloudlet = (Cloudlet) ev.getData();
        int vmId = selectVm();
        cloudlet.setVmId(vmId);
        processCloudletSubmit(ev, ack);
    }

    private int selectVm() {
        List<Vm> vmList = getVmList();
        if (currentVmIndex >= vmList.size()) {
            currentVmIndex = 0;
        }
        return vmList.get(currentVmIndex++).getId();
    }
}
