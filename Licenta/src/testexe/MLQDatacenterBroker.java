package testexe;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSimTags;

import java.util.ArrayList;
import java.util.List;

public class MLQDatacenterBroker extends DatacenterBroker {

    private List<Cloudlet> highPriorityCloudletList;
    private List<Cloudlet> mediumPriorityCloudletList;
    private List<Cloudlet> lowPriorityCloudletList;
    private int currentVmIndex;

    public MLQDatacenterBroker(String name) throws Exception {
        super(name);
        highPriorityCloudletList = new ArrayList<>();
        mediumPriorityCloudletList = new ArrayList<>();
        lowPriorityCloudletList = new ArrayList<>();
        currentVmIndex = 0;
    }

    @Override
    protected void processCloudletReturn(SimEvent ev) {
        Cloudlet cloudlet = (Cloudlet) ev.getData();
        getCloudletReceivedList().add(cloudlet);
        Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloudlet " + cloudlet.getCloudletId() + " received");
        cloudletsSubmitted--;
        if (getCloudletList().size() == 0 && cloudletsSubmitted == 0) {
            cloudletExecution();
        }
    }

    @Override
    protected void submitCloudlets() {
        if (getVmsCreatedList().size() == 0) {
            Log.printLine(getName() + ": No VMs available. Cannot submit cloudlets.");
            return;
        }
        prioritizeCloudlets();
        scheduleTasksToVms();
        submitCloudletsForExecution();
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
        // Do not clear the original cloudlet list to avoid losing cloudlets
    }

    protected void scheduleTasksToVms() {
        bindCloudletsToVms(highPriorityCloudletList);
        bindCloudletsToVms(mediumPriorityCloudletList);
        bindCloudletsToVms(lowPriorityCloudletList);
    }

    protected void bindCloudletsToVms(List<Cloudlet> cloudletList) {
        List<Vm> vms = getVmsCreatedList();
        for (Cloudlet cloudlet : cloudletList) {
            Vm chosenVm = getNextVm(vms);
            if (chosenVm != null) {
                bindCloudletToVm(cloudlet.getCloudletId(), chosenVm.getId());
                Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloudlet " + cloudlet.getCloudletId() + " is bound with VM " + chosenVm.getId());
            } else {
                Log.printLine(getName() + ": No available VM to bind Cloudlet " + cloudlet.getCloudletId());
            }
        }
    }

    private Vm getNextVm(List<Vm> vms) {
        if (vms.isEmpty()) {
            return null;
        }
        Vm vm = vms.get(currentVmIndex);
        currentVmIndex = (currentVmIndex + 1) % vms.size();
        return vm;
    }

    protected void submitCloudletsForExecution() {
        submitCloudletListForExecution(highPriorityCloudletList);
        submitCloudletListForExecution(mediumPriorityCloudletList);
        submitCloudletListForExecution(lowPriorityCloudletList);
    }

    protected void submitCloudletListForExecution(List<Cloudlet> cloudletList) {
        for (Cloudlet cloudlet : cloudletList) {
            submitCloudletToDatacenter(cloudlet);
        }
    }

    protected void submitCloudletToDatacenter(Cloudlet cloudlet) {
        Integer datacenterId = getVmsToDatacentersMap().get(cloudlet.getVmId());
        if (datacenterId != null) {
            sendNow(datacenterId, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
            cloudletsSubmitted++;
        } else {
            Log.printLine("No datacenter mapping found for VM " + cloudlet.getVmId());
        }
    }

    protected void cloudletExecution() {
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
