package mytool

import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Named
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.useHandleUnchecked
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
    @Named("sourceJdbi") private val sourceJdbi: Jdbi,
) {
    fun dummyInsert() {
        createTable(sourceJdbi)
        val startTime = System.currentTimeMillis()
        insertAddresses(sourceJdbi)
        val endTime = System.currentTimeMillis()

        println("Inserted 1,000,000 addresses in ${(endTime - startTime) / 1000.0} seconds")
    }

    fun createTable(jdbi: Jdbi) {
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

        jdbi.useHandleUnchecked {
            it.execute(createTableSQL)
        }
    }

    fun insertAddresses(jdbi: Jdbi) {
        val insertSQL =
            """
            INSERT INTO addresses (street_address, city, state, postal_code, country)
            VALUES (?, ?, ?, ?, ?)
            """.trimIndent()

        jdbi.useHandleUnchecked { handler ->
            for (i in 1..10000) {
                val insert = handler.createUpdate(insertSQL)
                val address = generateDummyAddress()
                insert.bind(1, address.streetAddress)
                insert.bind(2, address.city)
                insert.bind(3, address.state)
                insert.bind(4, address.postalCode)
                insert.bind(5, address.country)
                insert.execute()
            }
        }
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
