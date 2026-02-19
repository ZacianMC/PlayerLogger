package de.Zacian.playerlogger;

import de.Zacian.playerlogger.storage.Database;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerCommandPreprocessEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.plugin.java.JavaPlugin;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;

public final class PlayerLogger extends JavaPlugin implements Listener {

    private Database db;
    private final Map<UUID, Long> sessionStartMs = new ConcurrentHashMap<>();
    private de.Zacian.playerlogger.i18n.Messages messages;
    public de.Zacian.playerlogger.i18n.Messages messages() { return messages; }

    @Override
    public void onEnable() {
        saveDefaultConfig();
        reloadConfig();
        messages = new de.Zacian.playerlogger.i18n.Messages(this);
        messages.reload();

        db = new Database(this);
        db.connect();
        db.initSchema();

        Bukkit.getPluginManager().registerEvents(this, this);

        var cmd = new de.Zacian.playerlogger.commands.PlayerLoggerCommand(this, db);

        this.registerCommand(
                "playerlogger",
                "PlayerLogger main command",
                List.of("plog"),
                cmd
        );

        getLogger().info("PlayerLogger enabled.");
    }



    @Override
    public void onDisable() {
        // offene Sessions sauber schließen
        if (getConfig().getBoolean("logging.join-quit", true)) {
            for (Player p : Bukkit.getOnlinePlayers()) {
                handleQuit(p.getUniqueId(), p.getName(), true);
            }
        }
        if (db != null) db.flush();
        if (db != null) db.shutdown();
        getLogger().info("PlayerLogger disabled.");
    }

    @EventHandler
    public void onJoin(PlayerJoinEvent e) {
        if (!getConfig().getBoolean("logging.join-quit", true)) {
            return;
        }
        Player p = e.getPlayer();
        UUID uuid = p.getUniqueId();
        String name = p.getName();
        long now = Instant.now().toEpochMilli();

        sessionStartMs.put(uuid, now);

        db.runAsync(() -> {
            upsertPlayer(uuid, name, now, true);

            try (PreparedStatement ps = db.getConnection().prepareStatement(
                    "INSERT INTO sessions(uuid, join_time) VALUES(?, ?)")) {
                ps.setString(1, uuid.toString());
                ps.setLong(2, now);
                ps.executeUpdate();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    @EventHandler
    public void onQuit(PlayerQuitEvent e) {
        if (!getConfig().getBoolean("logging.join-quit", true)) {
            return;
        }
        Player p = e.getPlayer();
        handleQuit(p.getUniqueId(), p.getName(), false);
    }

    public Long getSessionStart(UUID uuid) {
        return sessionStartMs.get(uuid);
    }

    private void handleQuit(UUID uuid, String name, boolean sync) {
        long now = Instant.now().toEpochMilli();
        Long start = sessionStartMs.remove(uuid);
        long sessionMs = (start == null) ? 0L : Math.max(0L, now - start);

        Runnable task = () -> {
            try (PreparedStatement ps = db.getConnection().prepareStatement(
                    "UPDATE players SET name=?, last_seen=?, online=0, total_playtime_ms = total_playtime_ms + ? WHERE uuid=?")) {
                ps.setString(1, name);
                ps.setLong(2, now);
                ps.setLong(3, sessionMs);
                ps.setString(4, uuid.toString());
                ps.executeUpdate();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }

            // letzte offene Session schließen
            final String sqliteSql =
                    "UPDATE sessions SET leave_time=?, playtime_ms=? " +
                            "WHERE id = (SELECT id FROM sessions WHERE uuid=? AND leave_time IS NULL ORDER BY join_time DESC LIMIT 1)";
            final String mysqlSql =
                    "UPDATE sessions SET leave_time=?, playtime_ms=? " +
                            "WHERE id = (SELECT id FROM (SELECT id FROM sessions WHERE uuid=? AND leave_time IS NULL ORDER BY join_time DESC LIMIT 1) t)";
            String sql = db.isMySql() ? mysqlSql : sqliteSql;

            try (PreparedStatement ps = db.getConnection().prepareStatement(sql)) {
                ps.setLong(1, now);
                ps.setLong(2, sessionMs);
                ps.setString(3, uuid.toString());
                ps.executeUpdate();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        };

        if (sync) {
            db.runSync(task);
        } else {
            db.runAsync(task);
        }
    }

    @EventHandler(ignoreCancelled = true)
    public void onCommand(PlayerCommandPreprocessEvent e) {
        if (!getConfig().getBoolean("logging.commands", true)) return;

        String cmd = e.getMessage();

        // Optionaler Filter: sensible Commands nicht loggen
        if (getConfig().getBoolean("command-filter.enabled", true)) {
            for (String deny : getConfig().getStringList("command-filter.deny-prefixes")) {
                if (deny == null || deny.isBlank()) continue;
                if (cmd.regionMatches(true, 0, deny, 0, deny.length())) {
                    return; // nicht loggen
                }
            }
        }

        Player p = e.getPlayer();
        UUID uuid = p.getUniqueId();
        String name = p.getName();
        long now = Instant.now().toEpochMilli();

        db.runAsync(() -> {
            try (PreparedStatement ps = db.getConnection().prepareStatement(
                    "INSERT INTO command_logs(uuid, name, time, command) VALUES(?, ?, ?, ?)")) {
                ps.setString(1, uuid.toString());
                ps.setString(2, name);
                ps.setLong(3, now);
                ps.setString(4, cmd);
                ps.executeUpdate();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        });
    }


    private void upsertPlayer(UUID uuid, String name, long now, boolean online) {
        final String sqliteSql =
                "INSERT INTO players(uuid, name, first_seen, last_seen, online, total_playtime_ms) " +
                        "VALUES(?, ?, ?, ?, ?, 0) " +
                        "ON CONFLICT(uuid) DO UPDATE SET " +
                        "name=excluded.name, last_seen=excluded.last_seen, online=excluded.online";

        final String mysqlSql =
                "INSERT INTO players(uuid, name, first_seen, last_seen, online, total_playtime_ms) " +
                        "VALUES(?, ?, ?, ?, ?, 0) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "name=VALUES(name), last_seen=VALUES(last_seen), online=VALUES(online)";

        final String sql = db.isMySql() ? mysqlSql : sqliteSql;

        try (PreparedStatement ps = db.getConnection().prepareStatement(sql)) {
            ps.setString(1, uuid.toString());
            ps.setString(2, name);
            ps.setLong(3, now);
            ps.setLong(4, now);
            ps.setInt(5, online ? 1 : 0);
            ps.executeUpdate();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

}
