package com.facebook.presto.orc.zstd;

import com.facebook.airlift.configuration.Config;

public class ZstdConfig
{
    private Integer zstdCompressionLevel = 3;

    public Integer getZstdCompressionLevel()
    {
        return zstdCompressionLevel;
    }

    @Config("hive.zstd-compression-level")
    public ZstdConfig setZstdCompressionLevel(Integer zstdCompressionLevel)
    {
        this.zstdCompressionLevel = zstdCompressionLevel;
        return this;
    }
}
