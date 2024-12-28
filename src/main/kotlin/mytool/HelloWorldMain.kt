package mytool

import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.logging.Log
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.LinkedHashSet

@QuarkusMain
class HelloWorldMain(
    datasource: AgroalDataSource,
    @DataSource("users") usersDB: AgroalDataSource,
    private val dataImporter: DataImporter,
) : QuarkusApplication {
    val jdbi = Jdbi.create(datasource).installPlugin(KotlinPlugin(enableCoroutineSupport = true))
    val h2 = Jdbi.create(usersDB)

    override fun run(vararg args: String): Int {
        val arg = if (args.isNotEmpty()) args[0] else "default.db"
        Log.info("Hello $arg")
        if (arg == "import") {
            dataImporter.dummyInsert()
            return 0
        }
//        val duck = Jdbi.create("jdbc:duckdb:$arg").installPlugin(KotlinPlugin(enableCoroutineSupport = true))

// create a table
        h2.useHandleUnchecked {
            it.execute("DROP TABLE IF EXISTS addrx")
            it
                .execute(
                    """
        CREATE TABLE IF NOT EXISTS addrx (id INTEGER PRIMARY KEY,
            street_address VARCHAR(100),city VARCHAR(50),
            state VARCHAR(50),postal_code VARCHAR(20),country VARCHAR(50))""",
                )
        }

        dataLoadToDuck(
            jdbi,
            "select id,street_address,city,state,postal_code,country from addresses",
            "addrx",
            h2,
        )

        Log.info("Finish load")

        return 0
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private fun dataLoadToDuck(
        jdbi: Jdbi,
        sql: String,
        targetTbl: String,
        duck: Jdbi,
    ): AtomicInteger =
        runBlocking {
            val currentTime = System.currentTimeMillis()
            val cnt = AtomicInteger(0)
            val cntRev = AtomicInteger(0)
            // insert two items into the table
            Log.info("Start to load $targetTbl")
            val channel = Channel<Map<String, Any>>(1000000)
            val nameClassMap = mutableMapOf<String, Class<*>>()
            launch(Dispatchers.IO) {
                jdbi.withHandleUnchecked { handler ->
                    val flow =
                        handler
                            .createQuery(sql)
                            .map { rs, _ ->
                                val columnNames = LinkedHashSet<String>()
                                val meta = rs.metaData
                                val columnCount = meta.columnCount
                                for (i in 0..<columnCount) {
                                    val columnName = meta.getColumnName(i + 1)
                                    val alias = meta.getColumnLabel(i + 1)
                                    val name = (alias ?: columnName).lowercase()
                                    val added = columnNames.add(name)
                                    if (!added) {
                                        throw RuntimeException("column $name appeared twice in this resultset!")
                                    }
                                    if (!nameClassMap.containsKey(name)) {
                                        nameClassMap[name] = Class.forName(meta.getColumnClassName(i + 1))
                                        Log.info("Added $name class: ${nameClassMap[name]}")
                                    }
                                }
                                columnNames.withIndex().associate { (i, columnName) ->
                                    Pair(columnName, rs.getObject(i + 1))
                                }
                            }.asFlow()
                    runBlocking {
                        flow
                            .map { map ->
                                channel.send(map)
                            }.collect {
                                if ((cnt.getAndAdd(1) % 10000) == 0) {
                                    Log.info("Send $targetTbl $cnt")
                                }
                            }
                    }
                }
                channel.close()
                Log.info("Finish extract")
            }

            channel
                .consumeAsFlow()
                .chunked(2000)
                .collect { list ->
                    duck.withHandleUnchecked { hd ->
                        val batch =
                            hd.prepareBatch(
                                "INSERT INTO $targetTbl" +
                                    " (id, street_address, city, state, postal_code, country) " +
                                    " VALUES ( :id,:street_address,:city,:state,:postal_code,:country)",
                            )
                        list.forEach { map ->
                            map.forEach { (k, v) ->
                                batch.bindByType(k, v, nameClassMap[k])
                            }
                            batch.add()
                        }
                        batch.execute()
                    }
                    if ((cntRev.getAndAdd(list.size) % 10000) == 0) {
                        Log.info("Write $targetTbl $cntRev")
                    }
                }

            Log.info("Finish load in ${System.currentTimeMillis() / 1000 - currentTime / 1000}s")

            Log.info("All Send $targetTbl $cnt")
            Log.info("All Rec $targetTbl $cntRev")
            return@runBlocking cnt
        }
}
