package no.kartverket.matrikkel.broker.config

class DatabaseConfiguration(
    val env: MigrationEnv,
    val jdbcUrl: String,
    val userCredential: Credential,
    val adminCredential: Credential,
)

enum class MigrationEnv(
    val location: Array<String>
) {
    LOCAL(arrayOf("db/migration", "db/migration-test")),
    PROD(arrayOf("db/migration", "db/migration-prod"))
}

class Configuration(
    val database: DatabaseConfiguration = DatabaseConfiguration(
        env = MigrationEnv.valueOf(getConfig("DB_ENV") ?: MigrationEnv.PROD.name),
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