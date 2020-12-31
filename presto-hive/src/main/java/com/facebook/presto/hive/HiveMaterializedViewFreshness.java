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
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static com.facebook.presto.hive.metastore.MetastoreUtil.checkCondition;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class HiveMaterializedViewFreshness
{
    private final Table view;
    private final List<Table> baseTables;
    private final PartitionSpecs partitionsFromView;
    private final Map<SchemaTableName, PartitionSpecs> partitionsFromBaseTables;

    public HiveMaterializedViewFreshness(Table view, List<Table> baseTables, SemiTransactionalHiveMetastore hiveMetastore)
    {
        this.view = requireNonNull(view, "viewName is null");
        this.baseTables = ImmutableList.copyOf(requireNonNull(baseTables, "baseTableNames is null"));

        this.partitionsFromView = getPartitionSpecs(this.view, hiveMetastore);
        // Partitions to keep track of for materialized view freshness are the partitions of every base table
        // that are not available/updated to the materialized view yet. Compute it by partition specs "difference".
        this.partitionsFromBaseTables = baseTables.stream()
                .collect(toImmutableMap(
                        baseTable -> new SchemaTableName(baseTable.getDatabaseName(), baseTable.getTableName()),
                        baseTable -> PartitionSpecs.difference(getPartitionSpecs(baseTable, hiveMetastore), this.partitionsFromView)));
    }

    public static HiveMaterializedViewFreshness of(Table view, List<Table> baseTables, SemiTransactionalHiveMetastore hiveMetastore)
    {
        return new HiveMaterializedViewFreshness(view, baseTables, hiveMetastore);
    }

    public boolean isFresh()
    {
        // Materialized view is fresh if all base tables do not have more partitions than the materialized view.
        // When partitionsFromView is empty, which is the case when the materialized view is just created,
        // we treat it to be not fresh if any partitionsFromBaseTables is not empty.
        // It will become fresh once the first new partition lands.
        return Iterables.all(this.partitionsFromBaseTables.values(), PartitionSpecs::isEmpty);
    }

    private PartitionSpecs getPartitionSpecs(Table table, SemiTransactionalHiveMetastore metastore)
    {
        List<Column> keys = table.getPartitionColumns();
        // Hive Metastore getPartitionNames API returns partition names in natural order.
        List<List<String>> partitionsInOrder = metastore.getPartitionNames(table.getDatabaseName(), table.getTableName())
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(table.getDatabaseName(), table.getTableName())))
                .stream()
                .map(MetastoreUtil::toPartitionValues)
                .collect(toImmutableList());
        ImmutableList.Builder<List<HivePartitionKey>> partitionSpecs = ImmutableList.builder();
        for (List<String> values : partitionsInOrder) {
            checkCondition(keys.size() == values.size(), HIVE_INVALID_METADATA, "Expected %s partition key values, but got %s", keys.size(), values.size());
            ImmutableList.Builder<HivePartitionKey> partitionKeys = ImmutableList.builder();
            for (int i = 0; i < keys.size(); i++) {
                String name = keys.get(i).getName();
                HiveType hiveType = keys.get(i).getType();
                if (!hiveType.isSupportedType()) {
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type %s found in partition keys of table %s.%s", hiveType, table.getDatabaseName(), table.getTableName()));
                }
                String value = values.get(i);
                checkCondition(value != null, HIVE_INVALID_PARTITION_VALUE, "partition key value cannot be null for field: %s", name);
                partitionKeys.add(new HivePartitionKey(name, value));
            }
            partitionSpecs.add(partitionKeys.build());
        }

        return new PartitionSpecs(partitionSpecs.build(), keys);
    }

    private static final class PartitionSpecs
    {
        private final List<List<HivePartitionKey>> partitions;
        private final List<Column> keys;

        public PartitionSpecs(List<List<HivePartitionKey>> partitionSpecs, List<Column> keys)
        {
            this.partitions = ImmutableList.copyOf(partitionSpecs);
            this.keys = ImmutableList.copyOf(keys);
        }

        public static PartitionSpecs empty()
        {
            return new PartitionSpecs(Collections.emptyList(), Collections.emptyList());
        }

        public static PartitionSpecs difference(PartitionSpecs left, PartitionSpecs right)
        {
            if (right.partitions.isEmpty()) {
                return left;
            }
            Sets.SetView<String> commonKeys = Sets.intersection(
                    left.keys.stream().map(Column::getName).collect(toImmutableSet()),
                    right.keys.stream().map(Column::getName).collect(toImmutableSet()));
            if (commonKeys.isEmpty()) {
                return empty();
            }
            Function<List<HivePartitionKey>, Map<String, String>> toSpecOnCommonKeys = partSpec -> partSpec.stream()
                    .filter(hpk -> commonKeys.contains(hpk.getName()))
                    .collect(toImmutableMap(HivePartitionKey::getName, HivePartitionKey::getValue));

            Set<Map<String, String>> rightOnCommonKeys = right.partitions
                    .stream()
                    .map(toSpecOnCommonKeys)
                    .collect(toImmutableSet());

            List<List<HivePartitionKey>> diff = new ArrayList<>();
            List<HivePartitionKey> rightFirst = right.partitions.iterator().next();
            Iterator<List<HivePartitionKey>> leftIter = left.partitions.iterator();
            while (leftIter.hasNext()) {
                if (toSpecOnCommonKeys.apply(leftIter.next()).equals(toSpecOnCommonKeys.apply(rightFirst))) {
                    break;
                }
            }
            while (leftIter.hasNext()) {
                List<HivePartitionKey> leftSpec = leftIter.next();
                if (!rightOnCommonKeys.contains(toSpecOnCommonKeys.apply(leftSpec))) {
                    diff.add(leftSpec);
                }
            }
            return new PartitionSpecs(diff, left.keys);
        }

        public boolean isEmpty()
        {
            return partitions.isEmpty();
        }
    }
}
