/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.util;

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.IncrementalScanEvent;

public class EventParser {
    private static final String TABLE_NAME = "table-name";
    private static final String SNAPSHOT_ID = "snapshot-id";
    private static final String PROJECTION = "projection";
    private static final String OPERATION = "operation";
    private static final String SEQUENCE_NUMBER = "sequence-number";
    private static final String SUMMARY = "summary";
    private static final String FROM_SNAPSHOT_ID = "from-snapshot-id";
    private static final String TO_SNAPSHOT_ID = "to-snapshot-id";

    private EventParser() {
    }

    public static void toJson(ScanEvent scan, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeFieldName(TABLE_NAME);
        generator.writeString(scan.tableName());
        generator.writeFieldName(SNAPSHOT_ID);
        generator.writeNumber(scan.snapshotId());
        generator.writeFieldName(PROJECTION);
        generator.writeString(scan.projection().toString());
        generator.writeEndObject();
    }

    public static void toJson(CreateSnapshotEvent snapshot, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeFieldName(TABLE_NAME);
        generator.writeString(snapshot.tableName());
        generator.writeFieldName(OPERATION);
        generator.writeString(snapshot.operation());
        generator.writeFieldName(SNAPSHOT_ID);
        generator.writeNumber(snapshot.snapshotId());
        generator.writeFieldName(SEQUENCE_NUMBER);
        generator.writeNumber(snapshot.sequenceNumber());
        generator.writeFieldName(SUMMARY);
        generator.writeString(snapshot.summary().toString());
        generator.writeEndObject();
    }

    public static void toJson(IncrementalScanEvent incrementalScan, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeFieldName(TABLE_NAME);
        generator.writeString(incrementalScan.tableName());
        generator.writeFieldName(FROM_SNAPSHOT_ID);
        generator.writeNumber(incrementalScan.fromSnapshotId());
        generator.writeFieldName(TO_SNAPSHOT_ID);
        generator.writeNumber(incrementalScan.toSnapshotId());
        generator.writeFieldName(PROJECTION);
        generator.writeString(incrementalScan.projection().toString());
        generator.writeEndObject();
    }
}
