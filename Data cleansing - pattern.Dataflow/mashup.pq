[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "TEST_PATTERNS scraped_data_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"TEST_PATTERNS scraped_data" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "7a798b59-e76b-4af6-9ffb-fa0a2959519c"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2fc9f370-5018-46eb-a774-9ed60d52a11b"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "TEST_PATTERNS.scraped_data", ItemKind = "Table"]}[Data],
  #"Removed blank rows" = Table.SelectRows(#"Navigation 2", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Sorted rows" = Table.Sort(#"Removed blank rows", {{"title", Order.Ascending}})
in
  #"Sorted rows";
shared #"TEST_PATTERNS scraped_data_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "7a798b59-e76b-4af6-9ffb-fa0a2959519c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "2fc9f370-5018-46eb-a774-9ed60d52a11b"]}[Data],
  TableNavigation = Navigation_2{[Id = "TEST_PATTERNS.scraped_cleaned_data", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "7a798b59-e76b-4af6-9ffb-fa0a2959519c"]}[Data]{[lakehouseId = "2fc9f370-5018-46eb-a774-9ed60d52a11b"]}[Data];
