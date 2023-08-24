package io.numaproj.numaflow.examples.sideinput.udf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

/**
  SideInputWatcher is used to watch for side input file changes.
 */
public class SideInputWatcher {

    private final String fileName;
    private final String dirPath;
    private WatchService watchService;
    private Thread watchThread;
    private String content = ""; // Shared variable to store the content

    public SideInputWatcher(String dirPath, String fileName) {
        this.dirPath = dirPath;
        this.fileName = fileName;
        try {
            this.watchService = FileSystems.getDefault().newWatchService();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void startWatching() {
        try {
            Path path = Paths.get(dirPath);
            path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);

            watchThread = new Thread(() -> {
                try {
                    while (true) {
                        WatchKey key = watchService.take();
                        for (WatchEvent<?> event : key.pollEvents()) {
                            if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
                                Path modifiedFile = (Path) event.context();
                                // Check if the modified file is the same as the side input file
                                if (modifiedFile.toString().equals(fileName)) {
                                    handleFileUpdate();
                                }
                            }
                        }
                        key.reset();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });

            watchThread.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleFileUpdate() {
        Path filePath = Paths.get(dirPath, fileName);
        try {
            String content = readFileContents(filePath);
            synchronized (this) {
                this.content = content;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String readFileContents(Path filePath) throws IOException {
        StringBuilder contentBuilder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath.toFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                contentBuilder.append(line).append("\n");
            }
        }
        return contentBuilder.toString();
    }

    public synchronized String getSideInput() {
        return content;
    }

    public void stopWatching() {
        if (watchThread != null) {
            watchThread.interrupt();
        }
    }
}
