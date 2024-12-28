package mytool

import io.agroal.api.AgroalDataSource
import io.quarkus.logging.Log
import jakarta.enterprise.context.ApplicationScoped
import java.sql.Connection
import kotlin.random.Random

data class Address(
    val streetAddress: String,
    val city: String,
    val state: String,
    val postalCode: String,
    val country: String,
)

@ApplicationScoped
class DataImporter(
    private val dataSource: AgroalDataSource,
) {
    fun dummyInsert() {
        val connection = dataSource.connection

        try {
            createTable(connection)
            val startTime = System.currentTimeMillis()
            insertAddresses(connection)
            val endTime = System.currentTimeMillis()

            println("Inserted 1,000,000 addresses in ${(endTime - startTime) / 1000.0} seconds")
        } finally {
            connection.close()
        }
    }

    fun createTable(connection: Connection) {
        val createTableSQL =
            """
            CREATE TABLE IF NOT EXISTS addresses (
                id SERIAL PRIMARY KEY,
                street_address VARCHAR(100),
                city VARCHAR(50),
                state VARCHAR(50),
                postal_code VARCHAR(20),
                country VARCHAR(50)
            )
            """.trimIndent()

        connection.createStatement().use { statement ->
            statement.execute(createTableSQL)
        }
    }

    fun insertAddresses(connection: Connection) {
        val insertSQL =
            """
            INSERT INTO addresses (street_address, city, state, postal_code, country)
            VALUES (?, ?, ?, ?, ?)
            """.trimIndent()

        connection.autoCommit = false
        connection.prepareStatement(insertSQL).use { statement ->
            for (i in 1..1_000_000) {
                val address = generateDummyAddress()
                statement.setString(1, address.streetAddress)
                statement.setString(2, address.city)
                statement.setString(3, address.state)
                statement.setString(4, address.postalCode)
                statement.setString(5, address.country)
                statement.addBatch()

                if (i % 10000 == 0) {
                    statement.executeBatch()
                    connection.commit()
                    Log.info("$i inserted")
                }
            }
            statement.executeBatch()
            connection.commit()
        }
        connection.autoCommit = true
    }

    fun generateDummyAddress(): Address {
        val streets = listOf("Main St", "Oak Ave", "Park Rd", "Cedar Ln", "Elm St")
        val cities = listOf("Springfield", "Rivertown", "Lakeside", "Hillview", "Maplewood")
        val states = listOf("CA", "NY", "TX", "FL", "IL")
        val countries = listOf("USA", "Canada", "UK", "Australia", "Germany")

        return Address(
            streetAddress = "${Random.nextInt(1, 9999)} ${streets.random()}",
            city = cities.random(),
            state = states.random(),
            postalCode = Random.nextInt(10000, 99999).toString(),
            country = countries.random(),
        )
    }
}
