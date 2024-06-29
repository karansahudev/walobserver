package org.example;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessor;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.WALObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.slf4j.Logger;

import java.util.Optional;

public class EchoWALObserver implements WALCoprocessor, WALObserver {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(EchoWALObserver.class);

    public void postWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx, RegionInfo info, WALKey key, WALEdit edit) {
        StringBuilder sb = new StringBuilder();
        sb.append("Edit Cells: ");

        for (Cell cell : edit.getCells()) {
            sb.append("Cell: [")
                    .append("Family: ").append(Bytes.toString(CellUtil.cloneFamily(cell))).append(", ")
                    .append("Qualifier: ").append(Bytes.toString(CellUtil.cloneQualifier(cell))).append(", ")
                    .append("Value: ").append(Bytes.toString(CellUtil.cloneValue(cell))).append(", ")
                    .append("Timestamp: ").append(cell.getTimestamp()).append("]; ");
        }

        log.info(sb.toString());
    }

    @Override
    public Optional<WALObserver> getWALObserver() {
        return Optional.of(this);
    }
}

