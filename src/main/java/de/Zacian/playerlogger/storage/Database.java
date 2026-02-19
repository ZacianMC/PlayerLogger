package de.Zacian.playerlogger.storage;

import org.bukkit.plugin.java.JavaPlugin;

import java.io.File;
import java.sql.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Database {
    private final JavaPlugin plugin;
    private final ExecutorService executor;
    private Connection connection;
    private String dbType;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    public Database(JavaPlugin plugin) {
        this.plugin = plugin;
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "PlayerLogger-DB");
            t.setDaemon(true);
            return t;
        });
    }

    public void connect() {
        dbType = plugin.getConfig().getString("database.type", "SQLITE").toUpperCase();
        String type = dbType;

        try {
            if ("SQLITE".equals(type)) {

                File dbFile = new File(plugin.getDataFolder(),
                        plugin.getConfig().getString("database.sqlite-file", "playerlogger.db"));
                dbFile.getParentFile().mkdirs();

                connection = DriverManager.getConnection("jdbc:sqlite:" + dbFile.getAbsolutePath());

            } else { // MYSQL

                String host = plugin.getConfig().getString("database.mysql.host");
                int port = plugin.getConfig().getInt("database.mysql.port");
                String db = plugin.getConfig().getString("database.mysql.database");
                String user = plugin.getConfig().getString("database.mysql.user");
                String pass = plugin.getConfig().getString("database.mysql.password");

                // 🔹 Hier wird die URL definiert
                String url = "jdbc:mysql://" + host + ":" + port + "/" + db +
                        "?useSSL=false" +
                        "&allowPublicKeyRetrieval=true" +
                        "&useUnicode=true" +
                        "&characterEncoding=utf8" +
                        "&serverTimezone=UTC";

                connection = DriverManager.getConnection(url, user, pass);
            }

            connection.setAutoCommit(true);

        } catch (SQLException e) {
            throw new RuntimeException("DB connection failed", e);
        }
    }


    public boolean isMySql() {
        return "MYSQL".equals(dbType);
    }

    public boolean isSqlite() {
        return "SQLITE".equals(dbType);
    }

    private void tryCreateIndex(Statement st, String sql) throws SQLException {
        try {
            st.executeUpdate(sql);
        } catch (SQLException e) {
            // Index existiert schon -> ignorieren
            // SQLite: "already exists"
            // MySQL: "Duplicate key name"
            String msg = e.getMessage().toLowerCase();
            if (msg.contains("already exists") || msg.contains("duplicate") || msg.contains("exists")) {
                return;
            }
            throw e;
        }
    }


    public void initSchema() {
        runAsync(() -> {
            try (Statement st = connection.createStatement()) {
                if (isMySql()) {
                    // MySQL / MariaDB
                    st.executeUpdate(
                            "CREATE TABLE IF NOT EXISTS players (" +
                                    "uuid VARCHAR(36) NOT NULL PRIMARY KEY," +
                                    "name VARCHAR(16) NOT NULL," +
                                    "first_seen BIGINT NOT NULL," +
                                    "last_seen BIGINT NOT NULL," +
                                    "online TINYINT(1) NOT NULL DEFAULT 0," +
                                    "total_playtime_ms BIGINT NOT NULL DEFAULT 0" +
                                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
                    );

                    st.executeUpdate(
                            "CREATE TABLE IF NOT EXISTS sessions (" +
                                    "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                                    "uuid VARCHAR(36) NOT NULL," +
                                    "join_time BIGINT NOT NULL," +
                                    "leave_time BIGINT NULL," +
                                    "playtime_ms BIGINT NULL," +
                                    "CONSTRAINT fk_sessions_players FOREIGN KEY(uuid) REFERENCES players(uuid)" +
                                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
                    );

                    st.executeUpdate(
                            "CREATE TABLE IF NOT EXISTS command_logs (" +
                                    "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                                    "uuid VARCHAR(36) NOT NULL," +
                                    "name VARCHAR(16) NOT NULL," +
                                    "time BIGINT NOT NULL," +
                                    "command TEXT NOT NULL" +
                                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
                    );

                    tryCreateIndex(st, "CREATE INDEX idx_sessions_uuid ON sessions(uuid)");
                    tryCreateIndex(st, "CREATE INDEX idx_sessions_leave_time ON sessions(leave_time)");

                    tryCreateIndex(st, "CREATE INDEX idx_command_uuid ON command_logs(uuid)");
                    tryCreateIndex(st, "CREATE INDEX idx_command_time ON command_logs(time)");
                    tryCreateIndex(st, "CREATE INDEX idx_command_uuid_time ON command_logs(uuid, time)");

                    tryCreateIndex(st, "CREATE INDEX idx_players_online ON players(online)");

                    st.executeUpdate("UPDATE players SET online = 0");

                } else {
                    // SQLite
                    st.executeUpdate(
                            "CREATE TABLE IF NOT EXISTS players (" +
                                    "uuid TEXT PRIMARY KEY," +
                                    "name TEXT NOT NULL," +
                                    "first_seen INTEGER NOT NULL," +
                                    "last_seen INTEGER NOT NULL," +
                                    "online INTEGER NOT NULL DEFAULT 0," +
                                    "total_playtime_ms INTEGER NOT NULL DEFAULT 0" +
                                    ")"
                    );

                    st.executeUpdate(
                            "CREATE TABLE IF NOT EXISTS sessions (" +
                                    "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                                    "uuid TEXT NOT NULL," +
                                    "join_time INTEGER NOT NULL," +
                                    "leave_time INTEGER," +
                                    "playtime_ms INTEGER," +
                                    "FOREIGN KEY(uuid) REFERENCES players(uuid) ON DELETE CASCADE" +
                                    ")"
                    );

                    st.executeUpdate(
                            "CREATE TABLE IF NOT EXISTS command_logs (" +
                                    "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                                    "uuid TEXT NOT NULL," +
                                    "name TEXT NOT NULL," +
                                    "time INTEGER NOT NULL," +
                                    "command TEXT NOT NULL" +
                                    ")"
                    );

                    st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_sessions_uuid ON sessions(uuid)");
                    st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_sessions_leave_time ON sessions(leave_time)");

                    st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_command_uuid ON command_logs(uuid)");
                    st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_command_time ON command_logs(time)");
                    st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_command_uuid_time ON command_logs(uuid, time)");

                    st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_players_online ON players(online)");

                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public Connection getConnection() {
        return connection;
    }

    public void runAsync(Runnable task) {
        if (shuttingDown.get()) {
            plugin.getLogger().warning("DB task rejected: shutdown in progress.");
            return;
        }
        executor.submit(() -> {
            try {
                task.run();
            } catch (Throwable t) {
                plugin.getLogger().severe("DB task failed: " + t.getMessage());
                t.printStackTrace();
            }
        });
    }

    public void runSync(Runnable task) {
        if (shuttingDown.get()) {
            plugin.getLogger().warning("DB sync task rejected: shutdown in progress.");
            return;
        }
        Future<?> f = executor.submit(() -> {
            try {
                task.run();
            } catch (Throwable t) {
                plugin.getLogger().severe("DB task failed: " + t.getMessage());
                t.printStackTrace();
            }
        });
        try {
            f.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            plugin.getLogger().severe("DB sync task failed: " + e.getMessage());
        }
    }

    public void flush() {
        if (shuttingDown.get()) return;
        Future<?> f = executor.submit(() -> { });
        try {
            f.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            plugin.getLogger().severe("DB flush failed: " + e.getMessage());
        }
    }

    public void shutdown() {
        shuttingDown.set(true);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                plugin.getLogger().warning("DB shutdown taking longer than expected.");
            }
        } catch (InterruptedException ignored) { }

        if (connection != null) {
            try { connection.close(); } catch (SQLException ignored) { }
        }
    }
}
