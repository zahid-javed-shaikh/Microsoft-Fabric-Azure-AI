[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dailyfuelprice_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Source.Name", DestinationColumnName = "Source.Name"], [SourceColumnName = "date", DestinationColumnName = "date"], [SourceColumnName = "city", DestinationColumnName = "city"], [SourceColumnName = "state", DestinationColumnName = "state"], [SourceColumnName = "fuel_type", DestinationColumnName = "fuel_type"], [SourceColumnName = "price", DestinationColumnName = "price"], [SourceColumnName = "unit", DestinationColumnName = "unit"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared dailyfuelprice = let
  Source = AzureStorage.Blobs("https://dubaipulserawdata.blob.core.windows.net/"),
  #"Navigation 1" = Source{[Name = "dailyfuelprice"]}[Data],
  #"Filtered hidden files" = Table.SelectRows(#"Navigation 1", each [Attributes]?[Hidden]? <> true),
  #"Added custom" = let
    rootPath = Text.TrimEnd(Value.Metadata(Value.Type(#"Filtered hidden files"))[FileSystemTable.RootPath]?, "/"),
    combinePaths = (path1, path2) => Text.Combine({Text.TrimEnd(path1, "/"), path2}, "/"),
    getRelativePath = (path, relativeTo) => Text.Middle(path, Text.Length(relativeTo) + 1)
in
    Table.AddColumn(#"Filtered hidden files", "Relative Path", each getRelativePath(combinePaths([Folder Path], [Name]), rootPath), type text),
  #"Invoke custom function" = Table.AddColumn(#"Added custom", "Transform file", each #"Transform file"([Content])),
  #"Renamed columns" = Table.RenameColumns(#"Invoke custom function", {{"Relative Path", "Source.Name"}}),
  #"Removed other columns" = Table.SelectColumns(#"Renamed columns", {"Source.Name", "Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"date", type date}, {"city", type text}, {"state", type text}, {"fuel_type", type text}, {"price", type number}, {"unit", type text}})
in
  #"Changed column type";
shared #"Sample file" = let
  Source = AzureStorage.Blobs("https://dubaipulserawdata.blob.core.windows.net/"),
  #"Navigation 1" = Source{[Name = "dailyfuelprice"]}[Data],
  #"Filtered hidden files" = Table.SelectRows(#"Navigation 1", each [Attributes]?[Hidden]? <> true),
  Navigation = #"Filtered hidden files"{0}[Content]
in
  Navigation;
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Parquet.Document(Parameter)
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Parquet.Document(Parameter)
in
  Source;
shared dailyfuelprice_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[displayName = "WH_Warehouse"]}[Data],
  Navigation_3 = Navigation_2{[Schema = "dbo"]}[Data],
  TableNavigation = Navigation_3{[Name = "dailyfuelprice"]}?[Data]?
in
  TableNavigation;
