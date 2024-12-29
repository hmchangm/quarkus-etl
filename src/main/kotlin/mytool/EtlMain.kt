package mytool

import io.quarkus.logging.Log
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import jakarta.inject.Named
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.flow.*
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import java.util.concurrent.atomic.AtomicInteger

@QuarkusMain
class EtlMain(
    @Named("sourceJdbi") val sourceJdbi: Jdbi,
    @Named("targetJdbi") val targetJdbi: Jdbi,
    private val loaderBean: LoaderBean,
    private val dataImporter: DataImporter,
) : QuarkusApplication {
    override fun run(vararg args: String): Int {
        if (args.isNotEmpty() && args[0] == "import") {
            dataImporter.dummyInsert()
            return 0
        }

        loaderBean.jobs().forEach { job ->
            Log.info("Start job ${job.name()}")
            dataLoad(
                sourceJdbi,
                job.extract(),
                targetJdbi,
                job.write(),
            )
            Log.info("End job ${job.name()}")
        }

        return 0
    }

    private fun dataLoad(
        srcJdbi: Jdbi,
        sql: String,
        destJdbi: Jdbi,
        destSql: String,
    ) = runBlocking {
        val currentTime = System.currentTimeMillis()
        val cnt = AtomicInteger(0)
        val cntRev = AtomicInteger(0)
        val channel = Channel<Map<String, Any>>(UNLIMITED)
        val classMapMapper = ClassMapMapper()

        launchExtract(channel, srcJdbi, sql, classMapMapper, cnt)
        val nameClassMap = classMapMapper.nameClassMap
        runLoader(channel, destJdbi, destSql, nameClassMap, cntRev)

        Log.info("Finish load in ${System.currentTimeMillis() / 1000 - currentTime / 1000}s")

        Log.info("All Send $cnt")
        Log.info("All Rec  $cntRev")
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun runLoader(
        channel: Channel<Map<String, Any>>,
        destJdbi: Jdbi,
        destSql: String,
        nameClassMap: MutableMap<String, Class<*>>,
        cntRev: AtomicInteger,
    ) {
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
    }

    private fun CoroutineScope.launchExtract(
        channel: Channel<Map<String, Any>>,
        srcJdbi: Jdbi,
        sql: String,
        classMapMapper: ClassMapMapper,
        cnt: AtomicInteger,
    ) {
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
    }
}
