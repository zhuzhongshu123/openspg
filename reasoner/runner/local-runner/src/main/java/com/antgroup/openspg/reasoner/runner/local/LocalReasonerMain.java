/*
 * Copyright 2023 Ant Group CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.
 */

package com.antgroup.openspg.reasoner.runner.local;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.FileAppender;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.antgroup.openspg.reasoner.catalog.impl.KgSchemaConnectionInfo;
import com.antgroup.openspg.reasoner.runner.local.model.LocalReasonerResult;
import com.antgroup.openspg.reasoner.runner.local.model.LocalReasonerTask;
import com.opencsv.CSVWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

@Slf4j
public class LocalReasonerMain {

  /** KGReasoner main */
  public static void main(String[] args) {
    doMain(args);
    System.exit(0);
  }

  public static void doMain(String[] args) {
    LocalReasonerTask task = parseArgs(args);
    if (null == task) {
      System.exit(1);
    }
    LocalReasonerRunner runner = new LocalReasonerRunner();
    LocalReasonerResult result = runner.run(task);
    if (null == result) {
      log.error("local runner return null");
      return;
    }
    if (StringUtils.isNotEmpty(result.getErrMsg())) {
      log.error(result.getErrMsg());
    }
    if (StringUtils.isNotEmpty(task.getOutputFile())) {
      writeOutputFile(result, task.getOutputFile());
    }
  }

  protected static void writeOutputFile(LocalReasonerResult result, String file) {
    Path path = Paths.get(file);
    try {
      if (Files.notExists(path.getParent())) {
        Files.createDirectories(path.getParent());
      }
      if (Files.exists(path)) {
        Files.delete(path);
      }
    } catch (IOException e) {
      log.error("write result file error, file=" + file, e);
      return;
    }

    if (StringUtils.isNotEmpty(result.getErrMsg())) {
      writeFile(path, result.getErrMsg());
    } else if (result.isGraphResult()) {
      // write graph result
      writeFile(path, result.toString());
    } else {
      // write csv
      writeCsv(path, result.getColumns(), result.getRows());
    }
  }

  protected static void writeCsv(Path path, List<String> columns, List<Object[]> rows) {
    List<String[]> allLines = new ArrayList<>(rows.size() + 1);
    allLines.add(columns.toArray(new String[] {}));
    for (Object[] rowObj : rows) {
      String[] row = new String[rowObj.length];
      for (int i = 0; i < rowObj.length; ++i) {
        if (null != rowObj[i]) {
          row[i] = String.valueOf(rowObj[i]);
        } else {
          row[i] = null;
        }
      }
      allLines.add(row);
    }

    CSVWriter csvWriter;
    try {
      csvWriter = new CSVWriter(new FileWriter(path.toString()));
      csvWriter.writeAll(allLines);
      csvWriter.close();
    } catch (IOException e) {
      log.error("csvwriter error, file=" + path, e);
    }
  }

  protected static void writeFile(Path path, String content) {
    try {
      Files.write(path, content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE);
    } catch (IOException e) {
      log.error("write result file error, file=" + path, e);
    }
  }

  protected static LocalReasonerTask parseArgs(String[] args) {
    Options options = getOptions();

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    long projectId;
    String dsl;
    String outputFile;
    String schemaUri;
    String graphStateClass;
    String graphLoaderClass;
    String graphStateUrl;
    List<List<String>> startIdList;
    Map<String, Object> params = new HashMap<>(3);
    try {
      cmd = parser.parse(options, args);

      String logFileName = cmd.getOptionValue(LOG_FILE_OPTION);
      setUpLogFile(logFileName);

      projectId = Long.parseLong(cmd.getOptionValue(PROJECT_ID_OPTION));

      dsl = cmd.getOptionValue(QUERY_OPTION);
      if (StringUtils.isEmpty(dsl)) {
        throw new ParseException("please provide query dsl!");
      }
      outputFile = cmd.getOptionValue(OUTPUT_OPTION);
      if (StringUtils.isEmpty(outputFile)) {
        outputFile = null;
      }
      schemaUri = cmd.getOptionValue(SCHEMA_URL_OPTION);
      if (StringUtils.isEmpty(schemaUri)) {
        throw new ParseException("please provide openspg schema uri!");
      }
      graphLoaderClass = cmd.getOptionValue(GRAPH_LOADER_CLASS_OPTION);
      graphStateClass = cmd.getOptionValue(GRAPH_STATE_CLASS_OPTION);
      graphStateUrl = cmd.getOptionValue(GRAPH_STORE_URL_OPTION);
      if (StringUtils.isEmpty(graphStateUrl)) {
        graphStateUrl = null;
      }
      String startIdListJson = cmd.getOptionValue(START_ID_OPTION);
      if (StringUtils.isBlank(startIdListJson)) {
        startIdList = Collections.emptyList();
      } else {
        startIdList = JSON.parseObject(startIdListJson, new TypeReference<List<List<String>>>() {});
      }
      String paramsJson = cmd.getOptionValue(PARAMs_OPTION);
      if (StringUtils.isNotEmpty(paramsJson)) {
        params = new HashMap<>(JSON.parseObject(paramsJson));
      }
    } catch (ParseException e) {
      log.error(e.getMessage());
      formatter.printHelp("ReasonerLocalRunner", options);
      return null;
    }

    LocalReasonerTask task = new LocalReasonerTask();
    task.setId(UUID.randomUUID().toString());
    task.setDsl(dsl);
    task.setOutputFile(outputFile);
    task.setConnInfo(new KgSchemaConnectionInfo(schemaUri, ""));
    task.setGraphLoadClass(graphLoaderClass);
    task.setGraphStateClassName(graphStateClass);
    task.setGraphStateInitString(graphStateUrl);
    task.setStartIdList(new ArrayList<>());
    task.addStartId(startIdList);
    params.put("projId", projectId);
    task.setParams(params);
    return task;
  }

  protected static final String PROJECT_ID_OPTION = "projectId";
  protected static final String QUERY_OPTION = "query";
  protected static final String OUTPUT_OPTION = "output";
  protected static final String SCHEMA_URL_OPTION = "schemaUrl";
  protected static final String GRAPH_STATE_CLASS_OPTION = "graphStateClass";
  protected static final String GRAPH_LOADER_CLASS_OPTION = "graphLoaderClass";
  protected static final String GRAPH_STORE_URL_OPTION = "graphStoreUrl";
  protected static final String START_ID_OPTION = "startIdList";
  protected static final String PARAMs_OPTION = "params";
  protected static final String LOG_FILE_OPTION = "logFile";

  protected static Options getOptions() {
    Options options = new Options();

    options.addRequiredOption(PROJECT_ID_OPTION, PROJECT_ID_OPTION, true, "project id");
    options.addRequiredOption(QUERY_OPTION, QUERY_OPTION, true, "query dsl string");
    options.addOption(OUTPUT_OPTION, OUTPUT_OPTION, true, "output file name");
    options.addRequiredOption(SCHEMA_URL_OPTION, SCHEMA_URL_OPTION, true, "schema url");
    options.addOption(
        GRAPH_STATE_CLASS_OPTION, GRAPH_STATE_CLASS_OPTION, true, "graph state class name");
    options.addOption(
        GRAPH_LOADER_CLASS_OPTION, GRAPH_LOADER_CLASS_OPTION, true, "graph loader class name");
    options.addRequiredOption(
        GRAPH_STORE_URL_OPTION, GRAPH_STORE_URL_OPTION, true, "graph store url");
    options.addOption(START_ID_OPTION, START_ID_OPTION, true, "start id list");
    options.addOption(PARAMs_OPTION, PARAMs_OPTION, true, "params");
    options.addOption(LOG_FILE_OPTION, LOG_FILE_OPTION, true, "log file name");
    return options;
  }

  protected static void setUpLogFile(String logFileName) {
    LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
    loggerContext.reset();

    PatternLayoutEncoder patternLayoutEncoder = new PatternLayoutEncoder();
    patternLayoutEncoder.setPattern("%d [%X{traceId}] [%X{rpcId}] [%t] %-5p %c{2} - %m%n");
    patternLayoutEncoder.setContext(loggerContext);
    patternLayoutEncoder.start();

    FileAppender<ILoggingEvent> fileAppender = null;
    if (StringUtils.isNotBlank(logFileName)) {
      fileAppender = new FileAppender<>();
      fileAppender.setFile(logFileName);
      fileAppender.setEncoder(patternLayoutEncoder);
      fileAppender.setContext(loggerContext);
      fileAppender.setAppend(false);
      fileAppender.start();
    }

    ConsoleAppender<ILoggingEvent> consoleAppender = new ConsoleAppender<>();
    consoleAppender.setEncoder(patternLayoutEncoder);
    consoleAppender.setContext(loggerContext);
    consoleAppender.start();

    Logger brpcLogger = loggerContext.getLogger("com.baidu.brpc");
    brpcLogger.setLevel(Level.ERROR);
    brpcLogger.setAdditive(false);
    if (fileAppender != null) {
      brpcLogger.addAppender(fileAppender);
    }
    brpcLogger.addAppender(consoleAppender);

    Logger dtflysLogger = loggerContext.getLogger("com.dtflys.forest");
    dtflysLogger.setLevel(Level.ERROR);
    dtflysLogger.setAdditive(false);
    if (fileAppender != null) {
      dtflysLogger.addAppender(fileAppender);
    }
    dtflysLogger.addAppender(consoleAppender);

    Logger rootLogger = loggerContext.getLogger("root");
    if (fileAppender != null) {
      rootLogger.addAppender(fileAppender);
    }
    rootLogger.addAppender(consoleAppender);
    rootLogger.setLevel(Level.INFO);
  }
}
