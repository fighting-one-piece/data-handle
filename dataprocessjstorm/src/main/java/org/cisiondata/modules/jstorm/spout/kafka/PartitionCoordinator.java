package org.cisiondata.modules.jstorm.spout.kafka;

import java.util.List;

public interface PartitionCoordinator {
    List<PartitionManager> getMyManagedPartitions();

    PartitionManager getManager(Partition partition);

    void refresh();
}
