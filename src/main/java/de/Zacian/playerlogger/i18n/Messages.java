package de.Zacian.playerlogger.i18n;

import org.bukkit.ChatColor;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public final class Messages {

    private final JavaPlugin plugin;
    private YamlConfiguration cfg;
    private String lang; // "de" / "en"

    public Messages(JavaPlugin plugin) {
        this.plugin = plugin;
    }

    public void reload() {
        lang = plugin.getConfig().getString("language", "de").toLowerCase();
        String file = "messages_" + lang + ".yml";

        try (var in = plugin.getResource(file)) {
            if (in == null) {
                throw new IllegalStateException("Missing resource: " + file);
            }
            cfg = YamlConfiguration.loadConfiguration(new InputStreamReader(in, StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load " + file, e);
        }
    }

    public String lang() {
        return lang;
    }

    public String raw(String key) {
        String s = cfg.getString(key);
        return s != null ? s : ("<missing:" + key + ">");
    }

    public String msg(String key) {
        return color(raw(key));
    }

    public String msg(String key, Map<String, String> vars) {
        String s = raw(key);
        for (var e : vars.entrySet()) {
            s = s.replace("%" + e.getKey() + "%", e.getValue());
        }
        return color(s);
    }

    private static String color(String s) {
        return ChatColor.translateAlternateColorCodes('&', s);
    }
}
