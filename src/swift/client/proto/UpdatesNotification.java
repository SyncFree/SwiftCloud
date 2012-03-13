package swift.client.proto;

import java.util.List;

import swift.crdt.operations.CRDTObjectOperationsGroup;

// TODO this probably requires some rework 
public class UpdatesNotification {
    protected List<CRDTObjectOperationsGroup> objectUpdateGroups;
}
