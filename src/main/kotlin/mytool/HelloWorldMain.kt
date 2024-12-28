package mytool

import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.logging.Log
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.BUFFERED
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.flow.*
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import java.util.concurrent.atomic.AtomicInteger

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

        dataLoad(
            jdbi,
            "select id,street_address,city,state,postal_code,country from addresses",
            h2,
            "INSERT INTO addrx" +
                " (id, street_address, city, state, postal_code, country) " +
                " VALUES ( :id,:street_address,:city,:state,:postal_code,:country)",
        )

        return 0
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private fun dataLoad(
        srcJdbi: Jdbi,
        sql: String,
        destJdbi: Jdbi,
        destSql: String,
    ) = runBlocking {
        val currentTime = System.currentTimeMillis()
        val cnt = AtomicInteger(0)
        val cntRev = AtomicInteger(0)
        // insert two items into the table
        Log.info("Start to load ")
        val channel = Channel<Map<String, Any>>(UNLIMITED)
        val classMapMapper = ClassMapMapper()
        launch(Dispatchers.IO) {
            srcJdbi.withHandleUnchecked { handler ->
                val flow =
                    handler
                        .createQuery(sql)
                        .map(classMapMapper)
                        .asFlow()
                runBlocking {
                    flow
                        .map { map ->
                            channel.send(map)
                        }.collect {
                            if ((cnt.getAndAdd(1) % 10000) == 0) {
                                Log.info("Send $cnt")
                            }
                        }
                }
            }
            channel.close()
            Log.info("Finish extract")
        }

        val nameClassMap = classMapMapper.nameClassMap
        channel
            .consumeAsFlow()
            .chunked(2000)
            .collect { list ->
                destJdbi.withHandleUnchecked { hd ->
                    val batch =
                        hd.prepareBatch(
                            destSql,
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
                    Log.info("Write $cntRev")
                }
            }

        Log.info("Finish load in ${System.currentTimeMillis() / 1000 - currentTime / 1000}s")

        Log.info("All Send $cnt")
        Log.info("All Rec  $cntRev")
    }
}
