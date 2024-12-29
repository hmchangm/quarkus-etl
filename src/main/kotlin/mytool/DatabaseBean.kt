package mytool

import io.agroal.api.AgroalDataSource
import io.agroal.api.configuration.supplier.AgroalConnectionFactoryConfigurationSupplier
import io.agroal.api.configuration.supplier.AgroalConnectionPoolConfigurationSupplier
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier
import io.agroal.api.security.NamePrincipal
import io.agroal.api.security.SimplePassword
import io.smallrye.config.ConfigMapping
import jakarta.enterprise.context.Dependent
import jakarta.inject.Named
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.statement.SqlStatements
import java.time.Duration

@Dependent
class DatabaseBean {
    // Alternative method that accepts connection parameters

    @ConfigMapping(prefix = "datasource.source")
    interface SourceDbConfig : DataSourceConfig

    @ConfigMapping(prefix = "datasource.target")
    interface TargetDbConfig : DataSourceConfig

    @Named("sourceJdbi")
    fun buildSourceJdbi(dbConfig: SourceDbConfig) = createJdbi(dbConfig)

    @Named("targetJdbi")
    fun buildTargetJdbi(dbConfig: TargetDbConfig) = createJdbi(dbConfig)

    interface DataSourceConfig {
        fun driver(): String

        fun url(): String

        fun username(): String

        fun password(): String
    }

    private fun createJdbi(config: DataSourceConfig): Jdbi =
        createCustomDataSource(config)
            .let {
                Jdbi.create(it).apply {
                    getConfig(SqlStatements::class.java).setUnusedBindingAllowed(true)
                }
            }

    private fun createCustomDataSource(config: DataSourceConfig): AgroalDataSource {
        val poolConfiguration =
            AgroalConnectionPoolConfigurationSupplier()
                .connectionFactoryConfiguration { cf: AgroalConnectionFactoryConfigurationSupplier ->
                    cf
                        .jdbcUrl(config.url())
                        .credential(NamePrincipal(config.username()))
                        .credential(SimplePassword(config.password()))
                        .connectionProviderClass(Class.forName(config.driver()))
                }.maxSize(5)
                .acquisitionTimeout(Duration.ofSeconds(15))
                .validationTimeout(Duration.ofSeconds(10))

        val configuration =
            AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration(poolConfiguration)

        return AgroalDataSource.from(configuration)
    }
}
