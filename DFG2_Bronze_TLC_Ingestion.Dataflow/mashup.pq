[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "yellow_tripdata_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared yellow_tripdata = let
    // --------------------------------------------------------
    // Parameters
    // --------------------------------------------------------

    Year = "2026",
    Month = "01",

    // --------------------------------------------------------
    // Build source filename
    // --------------------------------------------------------

    FileName =
        "yellow_tripdata_"
        & Year
        & "-"
        & Month
        & ".parquet",

    SourceUrl =
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        & FileName,

    // --------------------------------------------------------
    // Read Parquet
    // --------------------------------------------------------

    Source =
        Parquet.Document(
            Binary.Buffer(
                Web.Contents(SourceUrl)
            )
        ),

    // --------------------------------------------------------
    // Add technical metadata
    // --------------------------------------------------------

    AddSourceFile =
        Table.AddColumn(
            Source,
            "source_file",
            each FileName,
            type text
        ),

    AddIngestionTimestamp =
        Table.AddColumn(
            AddSourceFile,
            "ingestion_timestamp",
        each DateTimeZone.RemoveZone(DateTimeZone.UtcNow()),
        type datetime
        ),

    AddBatchId =
        Table.AddColumn(
            AddIngestionTimestamp,
            "batch_id",
            each Year & Month,
            type text
        )

in
    AddBatchId;
shared #"yellow_tripdata row count" = let
    RowCount = Table.RowCount(yellow_tripdata)
in
    RowCount;
shared yellow_tripdata_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "b48f3b95-6043-42fb-b6ab-2206ae98dab8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "d38bb853-1399-4986-88f4-580d9aa155f8"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "bronze_yellow_trips", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_taxi_zone_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared bronze_taxi_zone = let
    Source =
        Csv.Document(
            Binary.Buffer(
                Web.Contents(
                    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
                )
            ),
            [
                Delimiter = ",",
                Encoding = 65001,
                QuoteStyle = QuoteStyle.Csv
            ]
        ),

    PromoteHeaders =
        Table.PromoteHeaders(
            Source,
            [PromoteAllScalars = true]
        ),

    SetDataTypes =
        Table.TransformColumnTypes(
            PromoteHeaders,
            {
                {"LocationID", Int64.Type},
                {"Borough", type text},
                {"Zone", type text},
                {"service_zone", type text}
            }
        ),

    AddSourceFile =
        Table.AddColumn(
            SetDataTypes,
            "source_file",
            each "taxi_zone_lookup.csv",
            type text
        ),

AddIngestionTimestamp =
    Table.AddColumn(
        AddSourceFile,
        "ingestion_timestamp",
        each DateTimeZone.RemoveZone(DateTimeZone.UtcNow()),
        type datetime
    ),

    AddBatchId =
        Table.AddColumn(
            AddIngestionTimestamp,
            "batch_id",
            each "20260831_ZONE",
            type text
        )

in
    AddBatchId;
shared bronze_taxi_zone_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "b48f3b95-6043-42fb-b6ab-2206ae98dab8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "d38bb853-1399-4986-88f4-580d9aa155f8"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "bronze_taxi_zone", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
