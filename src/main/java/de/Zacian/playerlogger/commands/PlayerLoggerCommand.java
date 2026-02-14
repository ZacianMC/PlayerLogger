package de.Zacian.playerlogger.commands;

import de.Zacian.playerlogger.storage.Database;
import io.papermc.paper.command.brigadier.BasicCommand;
import io.papermc.paper.command.brigadier.CommandSourceStack;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.OfflinePlayer;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.UUID;



public final class PlayerLoggerCommand implements BasicCommand {

    private final de.Zacian.playerlogger.PlayerLogger plugin;
    private final Database db;

    public PlayerLoggerCommand(de.Zacian.playerlogger.PlayerLogger plugin, Database db) {
        this.plugin = plugin;
        this.db = db;
    }

    @Override
    public void execute(CommandSourceStack source, String[] args) {
        CommandSender sender = source.getSender();

        if (args.length == 0) {
            sendHelp(sender);
            return;
        }

        String sub = args[0].toLowerCase(Locale.ROOT);

        switch (sub) {
            case "stats" -> handleStats(sender, args);
            case "top" -> handleTop(sender);
            case "reload" -> handleReload(sender);
            case "delete" -> handleDelete(sender, args);
            case "add" -> handleAdd(sender, args);
            default -> sendHelp(sender);
        }
    }

    @Override
    public Collection<String> suggest(CommandSourceStack source, String[] args) {
        if (args.length == 0) return List.of("stats", "top", "reload", "delete", "add");
        if (args.length == 1) return filterPrefix(List.of("stats", "top", "reload", "delete", "add"), args[0]);

        if (args.length == 2) {
            String sub = args[0].toLowerCase(Locale.ROOT);
            if (sub.equals("stats") || sub.equals("delete") || sub.equals("add")) {
                return filterPrefix(onlineNames(), args[1]);
            }
        }

        if (args.length == 3 && args[0].equalsIgnoreCase("add")) {
            return filterPrefix(List.of("30m", "1h", "2h", "90m", "3600s"), args[2]);
        }

        return List.of();
    }

    @Override
    public String permission() {
        return null; // wir prüfen pro Subcommand
    }

    // -------------------------
    // Subcommands
    // -------------------------

    private void handleStats(CommandSender sender, String[] args) {
        if (args.length == 1) {
            // /pl stats -> eigene
            if (!(sender instanceof Player p)) {
                sender.sendMessage(prefix() + plugin.messages().msg("err.players_only"));
                return;
            }
            if (!sender.hasPermission("playerlogger.stats")) {
                sender.sendMessage(prefix() + plugin.messages().msg("err.no_permission",
                        Map.of("perm", "playerlogger.stats")));
                return;
            }
            queryAndSendStatsByUuid(sender, p.getUniqueId().toString(), p.getName());
            return;
        }

        // /pl stats <player>
        if (!sender.hasPermission("playerlogger.stats.other")) {
            sender.sendMessage(prefix() + plugin.messages().msg("err.no_permission",
                    Map.of("perm", "playerlogger.stats.other")));
            return;
        }

        String targetName = args[1];
        OfflinePlayer off = Bukkit.getOfflinePlayer(targetName);
        String uuid = off.getUniqueId() != null ? off.getUniqueId().toString() : null;

        if (uuid != null) {
            queryAndSendStatsByUuid(sender, uuid, off.getName() != null ? off.getName() : targetName);
        } else {
            queryAndSendStatsByName(sender, targetName);
        }
    }

    private void handleTop(CommandSender sender) {
        if (!sender.hasPermission("playerlogger.top")) {
            sender.sendMessage(prefix() + plugin.messages().msg("err.no_permission",
                    Map.of("perm", "playerlogger.top")));
            return;
        }

        db.runAsync(() -> {
            String sql = "SELECT name, total_playtime_ms FROM players ORDER BY total_playtime_ms DESC LIMIT 10";
            List<String> lines = new ArrayList<>();

            try (PreparedStatement ps = db.getConnection().prepareStatement(sql);
                 ResultSet rs = ps.executeQuery()) {

                int rank = 1;
                while (rs.next()) {
                    String name = rs.getString("name");
                    long ms = rs.getLong("total_playtime_ms");
                    lines.add(ChatColor.GRAY + "[" + rank + "] " + ChatColor.YELLOW + name +
                            ChatColor.GRAY + " - " + ChatColor.AQUA + formatDuration(ms));
                    rank++;
                }

            } catch (SQLException e) {
                lines.clear();
                lines.add(prefix() + plugin.messages().msg("err.db", Map.of("error", e.getMessage())));
            }

            Bukkit.getScheduler().runTask(plugin, () -> {
                sender.sendMessage(prefix() + plugin.messages().msg("top.title"));
                if (lines.isEmpty()) {
                    sender.sendMessage(prefix() + plugin.messages().msg("top.empty"));
                } else {
                    for (String l : lines) sender.sendMessage(prefix() + l);
                }
            });
        });
    }

    private void handleReload(CommandSender sender) {
        if (!sender.hasPermission("playerlogger.Admin.reload")) {
            sender.sendMessage(prefix() + plugin.messages().msg("err.no_permission",
                    Map.of("perm", "playerlogger.Admin.reload")));
            return;
        }

        plugin.reloadConfig();
        plugin.messages().reload(); // wichtig: Sprache neu laden

        sender.sendMessage(prefix() + plugin.messages().msg("reload.ok"));
    }

    private void handleDelete(CommandSender sender, String[] args) {
        if (!sender.hasPermission("playerlogger.Admin.delete")) {
            sender.sendMessage(prefix() + plugin.messages().msg("err.no_permission",
                    Map.of("perm", "playerlogger.Admin.delete")));
            return;
        }
        if (args.length < 2) {
            sender.sendMessage(prefix() + plugin.messages().msg("err.usage_delete"));
            return;
        }

        String targetName = args[1];
        OfflinePlayer off = Bukkit.getOfflinePlayer(targetName);
        String uuid = off.getUniqueId() != null ? off.getUniqueId().toString() : null;

        db.runAsync(() -> {
            try {
                int delPlayers = 0, delSessions = 0, delCommands = 0;

                if (uuid != null) {
                    delSessions = exec("DELETE FROM sessions WHERE uuid=?", uuid);
                    delCommands = exec("DELETE FROM command_logs WHERE uuid=?", uuid);
                    delPlayers = exec("DELETE FROM players WHERE uuid=?", uuid);
                } else {
                    // Fallback über Name (weniger zuverlässig)
                    delCommands = exec("DELETE FROM command_logs WHERE name=?", targetName);
                    delPlayers = exec("DELETE FROM players WHERE name=?", targetName);
                }

                int fp = delPlayers, fs = delSessions, fc = delCommands;
                Bukkit.getScheduler().runTask(plugin, () ->
                        sender.sendMessage(prefix() + plugin.messages().msg("delete.ok", Map.of(
                                "p", String.valueOf(fp),
                                "s", String.valueOf(fs),
                                "c", String.valueOf(fc)
                        ))));

            } catch (SQLException e) {
                Bukkit.getScheduler().runTask(plugin, () ->
                        sender.sendMessage(prefix() + plugin.messages().msg("err.db", Map.of("error", e.getMessage()))));
            }
        });
    }

    private void handleAdd(CommandSender sender, String[] args) {
        if (!sender.hasPermission("playerlogger.Admin.add")) {
            sender.sendMessage(prefix() + plugin.messages().msg("err.no_permission",
                    Map.of("perm", "playerlogger.Admin.add")));
            return;
        }
        if (args.length < 3) {
            sender.sendMessage(prefix() + plugin.messages().msg("err.usage_add"));
            sender.sendMessage(prefix() + plugin.messages().msg("err.time_hint"));
            return;
        }

        String targetName = args[1];
        long addMs;
        try {
            addMs = parseDurationToMs(args[2]);
        } catch (IllegalArgumentException ex) {
            sender.sendMessage(prefix() + plugin.messages().msg("err.invalid_time", Map.of("error", ex.getMessage())));
            return;
        }
        if (addMs <= 0) {
            sender.sendMessage(prefix() + plugin.messages().msg("err.time_positive"));
            return;
        }

        OfflinePlayer off = Bukkit.getOfflinePlayer(targetName);
        String uuid = off.getUniqueId() != null ? off.getUniqueId().toString() : null;
        if (uuid == null) {
            sender.sendMessage(prefix() + plugin.messages().msg("err.uuid_not_found"));
            return;
        }

        long now = System.currentTimeMillis();
        String finalName = off.getName() != null ? off.getName() : targetName;

        db.runAsync(() -> {
            try {
                upsertPlayerMinimal(uuid, finalName, now);

                try (PreparedStatement ps = db.getConnection().prepareStatement(
                        "UPDATE players SET total_playtime_ms = total_playtime_ms + ? WHERE uuid=?")) {
                    ps.setLong(1, addMs);
                    ps.setString(2, uuid);
                    ps.executeUpdate();
                }

                Bukkit.getScheduler().runTask(plugin, () ->
                        sender.sendMessage(prefix() + plugin.messages().msg("add.ok", Map.of(
                                "time", formatDuration(addMs),
                                "player", finalName
                        ))));

            } catch (SQLException e) {
                Bukkit.getScheduler().runTask(plugin, () ->
                        sender.sendMessage(prefix() + plugin.messages().msg("err.db", Map.of("error", e.getMessage()))));
            }
        });
    }

    // -------------------------
    // DB helper
    // -------------------------

    private void queryAndSendStatsByUuid(CommandSender sender, String uuid, String displayName) {
        db.runAsync(() -> {
            String sql = "SELECT name, online, total_playtime_ms, first_seen, last_seen FROM players WHERE uuid=?";
            try (PreparedStatement ps = db.getConnection().prepareStatement(sql)) {
                ps.setString(1, uuid);

                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        Bukkit.getScheduler().runTask(plugin, () ->
                                sender.sendMessage(prefix() + plugin.messages().msg("err.no_data", Map.of("player", displayName))));
                        return;
                    }

                    String name = rs.getString("name");
                    long basePlayMs = rs.getLong("total_playtime_ms");
                    boolean online = rs.getInt("online") == 1;

                    long playMs = basePlayMs;
                    Long sessionMs = null;

                    // Live-Session addieren (nur wenn online und Session-Start bekannt)
                    if (online) {
                        try {
                            UUID u = UUID.fromString(uuid);
                            Long start = plugin.getSessionStart(u);
                            if (start != null) {
                                long now = System.currentTimeMillis();
                                long liveSession = Math.max(0L, now - start);
                                sessionMs = liveSession;
                                playMs = basePlayMs + liveSession;
                            }
                        } catch (IllegalArgumentException ignored) {
                            // uuid kaputt -> ignoriere live
                        }
                    }

                    long firstSeen = rs.getLong("first_seen");
                    long lastSeen = rs.getLong("last_seen");

                    long finalPlayMs = playMs;
                    Long finalSessionMs = sessionMs;
                    boolean finalOnline = online;

                    String firstSeenStr = formatDateTime(firstSeen);
                    String lastSeenStr = formatDateTime(lastSeen);

                    Bukkit.getScheduler().runTask(plugin, () -> {
                        String onlineStr = finalOnline
                                ? plugin.messages().msg("online.yes")
                                : plugin.messages().msg("online.no");

                        sender.sendMessage(prefix() + plugin.messages().msg("stats.title", Map.of("player", name)));
                        sender.sendMessage(prefix() + plugin.messages().msg("stats.online", Map.of("value", onlineStr)));
                        sender.sendMessage(prefix() + plugin.messages().msg("stats.playtime", Map.of("time", formatDuration(finalPlayMs))));

                        if (finalSessionMs != null) {
                            sender.sendMessage(prefix() + plugin.messages().msg("stats.session", Map.of("time", formatDuration(finalSessionMs))));
                        }

                        sender.sendMessage(prefix() + plugin.messages().msg("stats.first_seen", Map.of("time", firstSeenStr)));
                        sender.sendMessage(prefix() + plugin.messages().msg("stats.last_seen", Map.of("time", lastSeenStr)));
                    });
                }

            } catch (SQLException e) {
                Bukkit.getScheduler().runTask(plugin, () ->
                        sender.sendMessage(prefix() + plugin.messages().msg("err.db", Map.of("error", e.getMessage()))));
            }
        });
    }


    private void queryAndSendStatsByName(CommandSender sender, String name) {
        db.runAsync(() -> {
            String sql = "SELECT uuid, name FROM players WHERE name=? LIMIT 1";
            try (PreparedStatement ps = db.getConnection().prepareStatement(sql)) {
                ps.setString(1, name);

                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        Bukkit.getScheduler().runTask(plugin, () ->
                                sender.sendMessage(prefix() + plugin.messages()
                                        .msg("err.no_data", Map.of("player", name))));
                        return;
                    }

                    String uuid = rs.getString("uuid");
                    String realName = rs.getString("name");
                    queryAndSendStatsByUuid(sender, uuid, realName);
                }

            } catch (SQLException e) {   // 👈 e ist nur hier gültig
                Bukkit.getScheduler().runTask(plugin, () ->
                        sender.sendMessage(prefix() + plugin.messages()
                                .msg("err.db", Map.of("error", e.getMessage()))));
            }
        });
    }


    private int exec(String sql, String param) throws SQLException {
        try (PreparedStatement ps = db.getConnection().prepareStatement(sql)) {
            ps.setString(1, param);
            return ps.executeUpdate();
        }
    }

    private void upsertPlayerMinimal(String uuid, String name, long now) throws SQLException {
        final String sqliteSql =
                "INSERT INTO players(uuid, name, first_seen, last_seen, online, total_playtime_ms) " +
                        "VALUES(?, ?, ?, ?, 0, 0) " +
                        "ON CONFLICT(uuid) DO UPDATE SET " +
                        "name=excluded.name, last_seen=excluded.last_seen";

        final String mysqlSql =
                "INSERT INTO players(uuid, name, first_seen, last_seen, online, total_playtime_ms) " +
                        "VALUES(?, ?, ?, ?, 0, 0) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "name=VALUES(name), last_seen=VALUES(last_seen)";

        String sql = db.isMySql() ? mysqlSql : sqliteSql;

        try (PreparedStatement ps = db.getConnection().prepareStatement(sql)) {
            ps.setString(1, uuid);
            ps.setString(2, name);
            ps.setLong(3, now);
            ps.setLong(4, now);
            ps.executeUpdate();
        }
    }

    // -------------------------
    // UI helpers
    // -------------------------

    private static final ZoneId ZONE = ZoneId.of("Europe/Berlin");

    private DateTimeFormatter dateFormatter() {
        var lang = plugin.messages().lang();
        Locale locale = lang.equals("en") ? Locale.ENGLISH : Locale.GERMAN;
        return DateTimeFormatter.ofPattern("EEEE dd.MM.yyyy, HH:mm:ss", locale)
                .withZone(ZoneId.of("Europe/Berlin"));
    }

    private String formatDateTime(long epochMs) {
        return dateFormatter().format(Instant.ofEpochMilli(epochMs));
    }


    private void sendHelp(CommandSender sender) {
        sender.sendMessage(prefix() + plugin.messages().msg("help.title"));
        sender.sendMessage(prefix() + plugin.messages().msg("help.stats.self"));
        sender.sendMessage(prefix() + plugin.messages().msg("help.stats.other"));
        sender.sendMessage(prefix() + plugin.messages().msg("help.top"));
        sender.sendMessage(prefix() + plugin.messages().msg("help.reload"));
        sender.sendMessage(prefix() + plugin.messages().msg("help.delete"));
        sender.sendMessage(prefix() + plugin.messages().msg("help.add"));
    }


    private String prefix() {
        return plugin.messages().msg("prefix");
    }

    private String formatDuration(long ms) {
        long totalSeconds = TimeUnit.MILLISECONDS.toSeconds(ms);

        long days = totalSeconds / 86400;
        long hours = (totalSeconds % 86400) / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;

        StringBuilder sb = new StringBuilder();

        boolean en = plugin.messages().lang().equals("en");

        if (days > 0) {
            if (en) {
                sb.append(days == 1 ? "1 day " : days + " days ");
            } else {
                sb.append(days == 1 ? "1 Tag " : days + " Tage ");
            }
        }

        if (en) {
            sb.append(String.format("%02dh %02dm %02ds", hours, minutes, seconds));
        } else {
            sb.append(String.format("%02dh %02dm %02ds", hours, minutes, seconds));
        }

        return sb.toString();
    }



    private static long parseDurationToMs(String input) {
        String s = input.trim().toLowerCase(Locale.ROOT);
        if (s.isEmpty()) throw new IllegalArgumentException("leer");

        long multiplier;
        if (s.endsWith("ms")) {
            multiplier = 1L;
            s = s.substring(0, s.length() - 2);
        } else if (s.endsWith("s")) {
            multiplier = 1000L;
            s = s.substring(0, s.length() - 1);
        } else if (s.endsWith("m")) {
            multiplier = 60_000L;
            s = s.substring(0, s.length() - 1);
        } else if (s.endsWith("h")) {
            multiplier = 3_600_000L;
            s = s.substring(0, s.length() - 1);
        } else {
            // Default: Minuten
            multiplier = 60_000L;
        }

        long val;
        try {
            val = Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("keine Zahl: " + input);
        }
        if (val < 0) throw new IllegalArgumentException("negativ");
        return val * multiplier;
    }

    private List<String> onlineNames() {
        return Bukkit.getOnlinePlayers().stream().map(Player::getName).toList();
    }

    private static List<String> filterPrefix(Collection<String> options, String token) {
        String t = token.toLowerCase(Locale.ROOT);
        List<String> out = new ArrayList<>();
        for (String o : options) {
            if (o.toLowerCase(Locale.ROOT).startsWith(t)) out.add(o);
        }
        return out;
    }
}