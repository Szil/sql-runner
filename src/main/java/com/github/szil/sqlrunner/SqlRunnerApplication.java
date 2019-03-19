package com.github.szil.sqlrunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.WatchEvent.Kind;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootApplication
public class SqlRunnerApplication implements CommandLineRunner {

	private static final Logger LOGGER = LoggerFactory.getLogger(SqlRunnerApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(SqlRunnerApplication.class, args);
	}

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Override
	public void run(String... args) throws Exception {
		File scriptFile = new File("script.sql");

		if (!scriptFile.isFile()) {
			scriptFile.createNewFile();
		} else if (!scriptFile.canRead()) {
			LOGGER.error("Cannot read script file.");
			System.exit(-1);
		}

		try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
			new File(System.getProperty("user.dir")).toPath().register(watcher, StandardWatchEventKinds.ENTRY_MODIFY);
			Thread currThread = Thread.currentThread();
			while (!currThread.isInterrupted() && currThread.isAlive()) {
				WatchKey key = watcher.poll(100, TimeUnit.MILLISECONDS);

				if (key == null) {
					Thread.yield();
					continue;
				}

				for (WatchEvent<?> event : key.pollEvents()) {
					Kind<?> kind = event.kind();

					@SuppressWarnings("unchecked")
					WatchEvent<Path> ev = (WatchEvent<Path>) event;
					Path filename = ev.context();

					if (kind == StandardWatchEventKinds.OVERFLOW) {
						Thread.yield();
						continue;
					} else if (kind == java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY
							&& filename.toString().equals(scriptFile.getName())) {
						doOnChange(scriptFile);
					}
					boolean valid = key.reset();
					if (!valid) {
						break;
					}
				}

				Thread.yield();
			}

		}

		LOGGER.debug("Application finished.");
	}

	private void doOnChange(File scriptFile) {
		try (FileInputStream fis = new FileInputStream(scriptFile)) {
			String script = getFileContent(fis, "UTF-8");

			if (script == null || script.isEmpty()) {
				return;
			}

			try {
				List<Map<String, Object>> result = jdbcTemplate.queryForList(script);
	
				LOGGER.info("Script result: \n {}", Arrays.toString(result.toArray()));
			} catch (DataAccessException sqlEx) {
				LOGGER.error("Could not execute query", sqlEx);
			}
		} catch (Exception ex) {
			LOGGER.error("Could not read script file.", ex);
		}
	}

	public static String getFileContent(FileInputStream fis, String encoding) throws IOException {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(fis, encoding))) {
			StringBuilder sb = new StringBuilder();
			String line;
			while ((line = br.readLine()) != null) {
				sb.append(line);
				sb.append('\n');
			}
			return sb.toString();
		}
	}

}
