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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.collect.ImmutableMap;
import io.trino.hive.thrift.metastore.Table;
import io.trino.hive.thrift.metastore.TableMeta;
import io.trino.hive.thrift.metastore.ThriftHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastoreClient.AlternativeCall;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.IntStream.range;
import static org.apache.thrift.TApplicationException.UNKNOWN_METHOD;
import static org.assertj.core.api.Assertions.assertThat;

public class TestThriftHiveMetastoreClient
{
    @Test
    public void testAlternativeCall()
            throws TException
    {
        AtomicInteger connectionCount = new AtomicInteger();
        AtomicInteger chosenOption = new AtomicInteger(Integer.MAX_VALUE);

        AlternativeCall<String> failure = () -> {
            throw new RuntimeException("yay! exception");
        };
        AlternativeCall<String> success1 = () -> "first";
        AlternativeCall<String> success2 = () -> "second";

        ThriftHiveMetastoreClient client = new ThriftHiveMetastoreClient(
                () -> {
                    connectionCount.incrementAndGet();
                    return new TTransportMock();
                },
                "dummy",
                Optional.empty(),
                new MetastoreSupportsDateStatistics(),
                new AtomicInteger(),
                new AtomicInteger(),
                new AtomicInteger(),
                new AtomicInteger(),
                new AtomicInteger(),
                new AtomicInteger());
        assertThat(connectionCount.get()).isEqualTo(1);

        assertThat(client.alternativeCall(_ -> false, chosenOption, success1, success2, failure))
                .isEqualTo("first");
        assertThat(connectionCount.get()).isEqualTo(1);
        assertThat(chosenOption.get()).isEqualTo(0);

        chosenOption.set(Integer.MAX_VALUE);
        assertThat(client.alternativeCall(_ -> false, chosenOption, failure, success1, success2))
                .isEqualTo("first");
        assertThat(connectionCount.get()).isEqualTo(2);
        assertThat(chosenOption.get()).isEqualTo(1);

        assertThat(client.alternativeCall(_ -> false, chosenOption, failure, success1, success2))
                .isEqualTo("first");
        // Alternative call should use chosenOption
        assertThat(connectionCount.get()).isEqualTo(2);
    }

    @Test
    public void testGetTableMetaFallbackForOldMetastore()
            throws TException
    {
        List<String> tableNames = range(0, 250)
                .mapToObj(i -> "table_" + i)
                .collect(toImmutableList());
        List<List<String>> requestedBatches = new ArrayList<>();

        ThriftHiveMetastoreClient client = new ThriftHiveMetastoreClient(
                TTransportMock::new,
                "dummy",
                Optional.empty(),
                new MetastoreSupportsDateStatistics(),
                new AtomicInteger(),
                // force the fallback, since the failed first alternative would reconnect and drop the mocked client
                new AtomicInteger(1),
                new AtomicInteger(),
                new AtomicInteger(),
                new AtomicInteger(),
                new AtomicInteger());
        client.client = (ThriftHiveMetastore.Iface) Proxy.newProxyInstance(
                ThriftHiveMetastore.Iface.class.getClassLoader(),
                new Class<?>[] {ThriftHiveMetastore.Iface.class},
                (_, method, args) -> switch (method.getName()) {
                    case "getTableMeta" -> throw new TApplicationException(UNKNOWN_METHOD, "Invalid method name: 'get_table_meta'");
                    case "getAllTables" -> {
                        assertThat(args[0]).isEqualTo("db");
                        yield tableNames;
                    }
                    case "getTableObjectsByName" -> {
                        @SuppressWarnings("unchecked")
                        List<String> names = (List<String>) args[1];
                        requestedBatches.add(names);
                        yield names.stream()
                                .map(name -> table(name, name.equals("table_0") ? ImmutableMap.of("comment", "Presto View") : ImmutableMap.of()))
                                .collect(toImmutableList());
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });

        List<TableMeta> tableMetas = client.getTableMeta("db");

        assertThat(tableMetas).extracting(TableMeta::getTableName).containsExactlyElementsOf(tableNames);
        assertThat(tableMetas).extracting(TableMeta::getDbName).containsOnly("db");
        assertThat(tableMetas).extracting(TableMeta::getTableType).containsOnly("VIRTUAL_VIEW");
        assertThat(tableMetas.get(0).getComments()).isEqualTo("Presto View");
        assertThat(tableMetas.get(1).isSetComments()).isFalse();
        assertThat(requestedBatches).extracting(List::size).containsExactly(100, 100, 50);
    }

    private static Table table(String name, Map<String, String> parameters)
    {
        Table table = new Table();
        table.setDbName("db");
        table.setTableName(name);
        table.setTableType("VIRTUAL_VIEW");
        table.setParameters(parameters);
        return table;
    }

    private static class TTransportMock
            extends TTransport
    {
        @Override
        public boolean isOpen()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void open()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
            // Do nothing
        }

        @Override
        public int read(byte[] bytes, int i, int i1)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write(byte[] bytes, int i, int i1)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TConfiguration getConfiguration()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateKnownMessageSize(long size)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkReadBytesAvailable(long numBytes)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void resetMessageSizeAndConsumedBytes(long size)
        {
            throw new UnsupportedOperationException();
        }
    }
}
