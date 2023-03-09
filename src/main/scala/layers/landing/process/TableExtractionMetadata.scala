package layers.landing.process

final case class TableExtractionMetadata(
    format: String,
    driver: String,
    url: String,
    user: String,
    password: String,
    tables: Array[TableMetadata]
)
