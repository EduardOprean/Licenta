package testexe;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSimTags;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MLQDatacenterBroker extends DatacenterBroker {

    private List<Cloudlet> highPriorityCloudletList;
    private List<Cloudlet> mediumPriorityCloudletList;
    private List<Cloudlet> lowPriorityCloudletList;

    public MLQDatacenterBroker(String name) throws Exception {
        super(name);
        highPriorityCloudletList = new ArrayList<>();
        mediumPriorityCloudletList = new ArrayList<>();
        lowPriorityCloudletList = new ArrayList<>();
    }

    @Override
    protected void processCloudletReturn(SimEvent ev) {
        Cloudlet cloudlet = (Cloudlet) ev.getData();
        getCloudletReceivedList().add(cloudlet);
        Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloudlet " + cloudlet.getCloudletId() + " received");
        cloudletsSubmitted--;
        if (getCloudletList().size() == 0 && cloudletsSubmitted == 0) {
            scheduleTasksToVms();
            cloudletExecution(cloudlet);
        }
    }

    protected void scheduleTasksToVms() {
        prioritizeCloudlets();
        bindCloudletsToVms(highPriorityCloudletList);
        bindCloudletsToVms(mediumPriorityCloudletList);
        bindCloudletsToVms(lowPriorityCloudletList);
    }

    protected void prioritizeCloudlets() {
        for (Cloudlet cloudlet : getCloudletList()) {
            if (cloudlet.getCloudletLength() > 1000) {
                highPriorityCloudletList.add(cloudlet);
            } else if (cloudlet.getCloudletLength() > 500 && cloudlet.getCloudletLength() <= 1000) {
                mediumPriorityCloudletList.add(cloudlet);
            } else {
                lowPriorityCloudletList.add(cloudlet);
            }
        }
    }

    protected void bindCloudletsToVms(List<Cloudlet> cloudletList) {
        List<Vm> vms = getVmList();
        for (Cloudlet cloudlet : cloudletList) {
            Vm chosenVm = findMaxMipsVm(vms);
            bindCloudletToVm(cloudlet.getCloudletId(), chosenVm.getId());
            Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloudlet " + cloudlet.getCloudletId() + " is bound with VM " + chosenVm.getId());
        }
    }

    protected Vm findMaxMipsVm(List<Vm> vms) {
        Vm maxMipsVm = vms.get(0);
        for (Vm vm : vms) {
            if (vm.getMips() > maxMipsVm.getMips()) {
                maxMipsVm = vm;
            }
        }
        return maxMipsVm;
    }

    protected void cloudletExecution(Cloudlet cloudlet) {
        if (getCloudletList().isEmpty() && cloudletsSubmitted == 0) {
            Log.printLine(CloudSim.clock() + ": " + getName() + ": All Cloudlets executed. Finishing...");
            clearDatacenters();
            finishExecution();
        } else if (!getCloudletList().isEmpty() && cloudletsSubmitted == 0) {
            clearDatacenters();
            createVmsInDatacenter(0);
        }
    }
}
