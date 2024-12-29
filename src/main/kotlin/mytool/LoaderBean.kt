package mytool

import io.smallrye.config.ConfigMapping

@ConfigMapping(prefix = "loader")
interface LoaderBean {
    fun jobs(): List<EtlConfig>

    interface EtlConfig {
        fun name(): String

        fun extract(): String

        fun write(): String
    }
}
