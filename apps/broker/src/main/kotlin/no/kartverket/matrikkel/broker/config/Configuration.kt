package no.kartverket.matrikkel.broker.config

class DatabaseConfiguration(
    val migrationFiles: MigrationLocations,
    val jdbcUrl: String,
    val userCredential: Credential,
    val adminCredential: Credential,
)

@JvmInline
value class MigrationLocations(val locations: Array<String>)

class Configuration(
    val database: DatabaseConfiguration = DatabaseConfiguration(
        migrationFiles = MigrationLocations(arrayOf("db/migration")),
        jdbcUrl = getRequiredConfig("DB_URL"),
        userCredential = Credential.from("DB_USER"),
        adminCredential = Credential.from("DB_ADMIN"),
    ),
)

class Credential(
    val username: String,
    val password: String,
) {
    companion object {
        fun from(name: String) = Credential(
            username = firstNonNullOf("${name}_USERNAME", "${name}_USER"),
            password = firstNonNullOf("${name}_PASSWORD"),
        )
    }
}

private fun getConfig(name: String): String? {
    return System.getProperty(name, System.getenv(name))
}

private fun getRequiredConfig(name: String): String {
    return requireNotNull(getConfig(name)) {
        "$name must be defined in java properties or environment"
    }
}

private fun firstNonNullOf(vararg name: String): String {
    return name.firstNotNullOf { getConfig(it) }
}