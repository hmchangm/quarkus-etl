package mytool

import io.quarkus.logging.Log
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.StatementContext
import java.sql.ResultSet

class ClassMapMapper:RowMapper<Map<String,Any>> {

    val nameClassMap = mutableMapOf<String, Class<*>>()

    override fun map(rs: ResultSet, ctx: StatementContext): Map<String, Any> {
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
                Log.info("Add $name class: ${nameClassMap[name]}")
            }
        }
        return columnNames.withIndex().associate { (i, columnName) ->
            Pair(columnName, rs.getObject(i + 1))
        }
    }

}