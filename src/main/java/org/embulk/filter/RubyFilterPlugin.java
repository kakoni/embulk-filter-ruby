package org.embulk.filter;

import java.util.HashMap;
import java.util.Map;

import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.spi.time.Timestamp;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;

import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

public class RubyFilterPlugin implements FilterPlugin {

    private static final Logger log = LoggerFactory.getLogger(RubyFilterPlugin.class);

    static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();
    static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

    public interface PluginTask extends Task {
        @Config("ruby_code")
        String getRubyCode();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema, FilterPlugin.Control control) {
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        Schema outputSchema = inputSchema;

        control.run(task.toTaskSource(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema,
                           Schema outputSchema, PageOutput output) {

        final PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);

        // Initialize GraalVM context for Ruby
        Context context = Context.newBuilder("ruby")
                .allowAllAccess(true)
                .out(System.out)
                .build();

        // Define the Ruby function with the user's code
        String rubyFunctionCode = "def process(record)\n" + task.getRubyCode() + "\nend";
        context.eval("ruby", rubyFunctionCode);

        // Retrieve the Ruby 'process' method
        Value rubyProcessFunction = context.getBindings("ruby").getMember("process");

        return new PageOutput() {
            private final PageReader pageReader = Exec.getPageReader(inputSchema);
            private final PageBuilder pageBuilder = Exec.getPageBuilder(Exec.getBufferAllocator(), outputSchema, output);

            @Override
            public void add(Page page)
            {
                pageReader.setPage(page);

                while (pageReader.nextRecord()) {
                    // Create a map to hold the current record's data
                    Map<String, Object> recordMap = new HashMap<>();

                    // Collect data from input columns
                    for (Column column : inputSchema.getColumns()) {
                        String columnName = column.getName();
                        Type type = column.getType();

                        if (pageReader.isNull(column)) {
                            recordMap.put(columnName, null);
                        } else if (type.equals(Types.STRING)) {
                            recordMap.put(columnName, pageReader.getString(column));
                        } else if (type.equals(Types.LONG)) {
                            recordMap.put(columnName, pageReader.getLong(column));
                        } else if (type.equals(Types.DOUBLE)) {
                            recordMap.put(columnName, pageReader.getDouble(column));
                        } else if (type.equals(Types.BOOLEAN)) {
                            recordMap.put(columnName, pageReader.getBoolean(column));
                        } else if (type.equals(Types.TIMESTAMP)) {
                            recordMap.put(columnName, pageReader.getTimestamp(column).toEpochMilli());
                        } else {
                            throw new UnsupportedOperationException("Unsupported type: " + type);
                        }
                    }

                    Value recordValue = context.asValue(recordMap);
                    Value result = rubyProcessFunction.execute(recordValue);

                    Map<String, Object> resultMap = result.as(Map.class);

                    // Write data to output columns
                    for (Column column : outputSchema.getColumns()) {
                        String columnName = column.getName();
                        Object value = resultMap.get(columnName);

                        if (value == null) {
                            pageBuilder.setNull(column);
                        } else if (column.getType().equals(Types.STRING)) {
                            pageBuilder.setString(column, value.toString());
                        } else if (column.getType().equals(Types.LONG)) {
                            pageBuilder.setLong(column, ((Number) value).longValue());
                        } else if (column.getType().equals(Types.DOUBLE)) {
                            pageBuilder.setDouble(column, ((Number) value).doubleValue());
                        } else if (column.getType().equals(Types.BOOLEAN)) {
                            pageBuilder.setBoolean(column, (Boolean) value);
                        } else if (column.getType().equals(Types.TIMESTAMP)) {
                            pageBuilder.setTimestamp(column, Timestamp.ofEpochMilli(((Number) value).longValue()));
                        } else {
                            throw new UnsupportedOperationException("Unsupported type: " + column.getType());
                        }
                    }

                    pageBuilder.addRecord();
                }
            }

            @Override
            public void finish()
            {
                pageBuilder.finish();
                context.close();
            }

            @Override
            public void close()
            {
                pageBuilder.close();
                context.close();
            }

        };
    }

}