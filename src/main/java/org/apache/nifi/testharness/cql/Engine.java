package org.apache.nifi.testharness.cql;

import java.util.Locale;

public enum Engine {

    CASSANDRA("cassandra:5.0", "cassandra", "/etc/cassandra/cassandra.yaml"),
    SCYLLADB("scylladb/scylla:2026.2", "scylla", "/etc/scylla/scylla.yaml");

    private final String defaultImage;
    private final String imageNameToken;
    private final String configContainerPath;

    Engine(final String defaultImage, final String imageNameToken, final String configContainerPath) {
        this.defaultImage = defaultImage;
        this.imageNameToken = imageNameToken;
        this.configContainerPath = configContainerPath;
    }

    public String defaultImage() {
        return defaultImage;
    }

    public String configContainerPath() {
        return configContainerPath;
    }

    public String imageNameToken() {
        return imageNameToken;
    }

    /**
     * Substring match against the engine's canonical image name (e.g. "cassandra", "scylla") - enough to
     * catch a mismatched --specific-image without maintaining an exhaustive list of every published tag.
     */
    public boolean isCompatibleImage(final String image) {
        return image.toLowerCase(Locale.ROOT).contains(imageNameToken);
    }
}
