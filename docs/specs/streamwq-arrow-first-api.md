# Simplify STREAM Access with an Arrow-First streamwq Interface

## Problem Statement

Users currently learn a client class, separate dataset readers, and several selection and analysis modules to retrieve STREAM data. Readers materialize pandas objects even when users want another format. Site discovery and observation access are not clearly distinguished, and spatial selection currently depends on pandas.

The goal is a smaller, predictable public interface: discover sites, retrieve metadata, and retrieve selected data as Arrow tables. Data selection should not require a dataframe framework, while format-specific analysis should be explicitly optional.

This document records the design accepted during the 2026-10-20 conversation and implementation refinements from issue #3, merged in PR #10. The metadata/catalog slice is implemented; the remaining migration is not complete.

## Solution

Rename the package to `streamwq`, documented with the import alias `stream`. Expose module-level `get_sites`, `get_metadata`, `get_data`, and `get_watersheds` functions. Users do not need to instantiate or configure a client for ordinary access.

All standard access functions return materialized `pyarrow.Table` objects. Users call Arrow's native conversion methods when appropriate. A dedicated geometry-aware conversion helper restores geometry and coordinate reference system information for GeoPandas users.

Keep site, spatial, date-range, and read-time quality selection in core. Place pandas-specific post-access analysis under `streamwq.helpers.pandas`. Make pandas and GeoPandas optional rather than prerequisites for core access.

This is an intentionally breaking migration. Do not retain old imports or pandas-returning reader compatibility wrappers.

## User Stories

1. As a STREAM user, I want one package named streamwq, so that I have a consistent entry point for accessing STREAM datasets.
2. As a notebook author, I want examples to use the alias stream, so that examples are short and recognizable.
3. As a new user, I want module-level access functions, so that I can retrieve data without first learning a client lifecycle.
4. As a data analyst, I want every standard access result to be an Arrow table, so that I can rely on one return contract.
5. As a pandas user, I want to call the native to_pandas method, so that conversion does not require another package-specific abstraction.
6. As an Arrow user, I want core access to work without pandas installed, so that my workflow does not depend on a dataframe framework I do not use.
7. As a user of another data framework, I want the unconverted Arrow result, so that I can choose a compatible downstream conversion.
8. As a researcher, I want site discovery to return unique STREAM_ID values, so that I can use a compact site selection in subsequent requests.
9. As a researcher, I want get_sites to return a one-column Arrow table, so that site discovery follows the same return contract as other access functions.
10. As a researcher, I want to pass the discovery result directly to get_data, so that I do not manually extract identifiers.
11. As a researcher, I want to supply one identifier or a sequence of identifiers, so that simple and multi-site requests are equally convenient.
12. As a researcher, I want to retrieve all site metadata without providing a selection, so that I can inspect the available monitoring locations.
13. As a researcher, I want to retrieve metadata for selected sites, so that I can describe the locations used in my analysis.
14. As a researcher, I want metadata selection to accept the same identifier input forms as data access, so that I do not learn different conventions for each function.
15. As a researcher, I want HUC-based discovery, so that I can select sites in a hydrologic region.
16. As a researcher, I want bounding-box discovery, so that I can select sites in a geographic extent.
17. As a researcher, I want radius-based discovery around a location, so that I can find nearby monitoring sites.
18. As a researcher, I want different discovery filters to combine with AND, so that returned sites satisfy every supplied constraint.
19. As a researcher, I want alternatives within an individual discovery filter to combine with OR, so that I can select any of several requested values.
20. As a researcher, I want discovery to use metadata rather than scan observations, so that finding candidate sites remains lightweight.
21. As a researcher, I want a clear error when discovery matches no sites, so that I can revise the selection immediately.
22. As a researcher, I want water-quality and streamflow access through the same function, so that changing datasets does not require a new reader interface.
23. As a researcher, I want other supported tabular datasets accessible through that function, so that the simplified interface does not discard existing capabilities.
24. As a researcher, I want an explicit dataset argument, so that a request does not silently select the wrong dataset.
25. As a researcher, I want data access to require site information, so that an omitted selection cannot accidentally trigger a full-dataset read.
26. As a researcher, I want missing, null, or empty data-access site selections to raise clear errors, so that invalid requests fail visibly.
27. As a researcher, I want any unknown site ID to reject the entire data request and identify the offending IDs, so that typos do not silently produce partial results.
28. As a researcher, I want valid queries with no matching observations to return an empty table with the expected schema, so that absence of observations is distinguishable from invalid site information.
29. As a researcher, I want explicit column selection, so that I can limit retrieved fields.
30. As a researcher, I want date-range selection during access, so that unnecessary observations are not materialized.
31. As a researcher, I want quality-flag selection during access, so that I can retrieve observations appropriate to my analysis.
32. As a researcher, I want filters unsupported by a dataset to raise errors, so that my requested constraints are never silently ignored.
33. As a spatial analyst, I want dedicated watershed access, so that geometry-specific requirements remain clear.
34. As a spatial analyst, I want watershed Arrow results to preserve geometry and coordinate reference system metadata, so that later conversion is spatially correct.
35. As a GeoPandas user, I want a geometry-aware conversion helper, so that I receive a usable GeoDataFrame rather than an ordinary dataframe of encoded geometries.
36. As a user who does not need GeoPandas, I want geometry conversion dependencies to be optional, so that core installation stays independent of them.
37. As a pandas analyst, I want water-year calculations and seasonal summaries in an explicitly pandas-specific helper module, so that input-format expectations are clear.
38. As a package user, I want to inspect dataset names and variables through a public catalog, so that valid access arguments are discoverable.
39. As a package maintainer, I want storage and connection details to remain private, so that public discovery does not expose those implementation contracts.
40. As a notebook author, I want maintained examples updated for the breaking migration, so that they demonstrate the supported interface rather than obsolete imports and return types.
41. As a future contributor, I want this agreed design and its unresolved details recorded, so that I can resume implementation in another conversation without reconstructing the interview.

## Implementation Decisions

- Package identity: rename the distribution and import package from stream-utils/stream_utils to streamwq. Documentation uses stream as the alias. The name was chosen deliberately despite the package also providing non-water-quality datasets.
- Common entry point: expose module-level functions; ordinary users do not instantiate a client. An explicit client interface for advanced configuration was not decided and must not be treated as an agreed requirement.
- Return contract: get_sites, get_metadata, get_data, and get_watersheds return materialized pyarrow.Table objects, not pandas objects, lazy datasets, scanners, streaming readers, or custom wrappers.
- Site identity: preserve the canonical data field STREAM_ID. Use site_ids for the Python selection argument, not Stream_ID. Both water-quality and streamflow access use the same site identifier.
- Site discovery: get_sites returns exactly one column, STREAM_ID, containing unique identifiers. It selects candidates from metadata; it does not promise observations exist for the requested dataset, dates, or quality conditions.
- Spatial discovery: expose spatial selection directly through get_sites, including HUC, bounding-box, and center/radius selection. Keep its implementation in core and remove its pandas dependency. Exact parameter names and validation details remain to be finalized against existing behavior.
- Discovery composition: different filters combine with AND; multiple alternative values within one filter combine with OR. Existing multi-value variable-availability checks must be reviewed against this agreed contract rather than assumed unchanged.
- Discovery failures: no matching sites raises a clear error rather than returning an empty discovery table. The exact exception class was not settled.
- Metadata: get_metadata replaces read_metadata. Omitting site_ids returns all site metadata. A supplied selection accepts a single ID, a sequence of IDs, or the one-column Arrow table produced by get_sites. An explicitly empty selection raises an error.
- Metadata details established by #3: the signature is get_metadata(site_ids=None, *, columns=None). None selects all sites. Empty selections, null elements, non-string IDs, blank IDs, and Arrow selections other than exactly one STREAM_ID column raise ValueError before storage access. Column projection preserves stored field types and schema metadata and can omit STREAM_ID while filtering by it. Repeated requested IDs do not duplicate stored rows; unknown IDs are filtered out, with an all-unknown selection returning a schema-preserving empty table. No result-order guarantee is added. These permissive metadata rules do not replace get_data's all-or-nothing identity validation.
- Connection lifecycle: metadata access lazily reuses an authenticated provider within the process; imports and catalog inspection do not connect, and metadata results are not cached. Future module-level readers should share private provider infrastructure to avoid repeated authentication across access functions. Its current placement in the metadata module is not a permanent architecture requirement or a new public client contract.
- Unified data access: get_data replaces the separate tabular dataset readers. It requires an explicit dataset argument and site_ids. Cover the currently supported water-quality, streamflow, static catchment attribute, grab-sample, meteorology, land-cover, and anthropogenic tabular datasets. Keep metadata and watershed geometry on their dedicated entry points. Public dataset spellings must be documented in the catalog, not inferred from this prose.
- Issue #5 implements get_data for water_quality and streamflow; later migration issues add the remaining tabular datasets. It accepts columns, start, end, and quality as explicit shared arguments. A supported catalog dataset not yet implemented by get_data raises ValueError rather than being silently ignored.
- Selection inputs: get_data accepts a single ID, a sequence of IDs, or the Arrow table returned by get_sites. Do not require users to extract a list from discovery results.
- Missing selection: get_data raises a clear ValueError for omitted, null, or empty site information. The implementation must deliberately honor this contract, rather than accidentally allowing Python's missing-argument TypeError to define it.
- Unknown IDs: get_data validates site identity against the site metadata, rejects the whole request if any supplied ID is unknown, and identifies the invalid IDs in the error. A site absent from one observation dataset is not automatically an unknown site.
- No observations: valid sites with no observations matching the dataset and filters return a zero-row Arrow table with the schema corresponding to the requested projection. Do not confuse this with missing site information or failed discovery.
- Data filters: expose explicit shared columns, start, end, and quality arguments. Reject arguments that do not apply to the selected dataset; for example, date-range filters on static attributes. Do not silently ignore unsupported arguments or use an unstructured catch-all to conceal dataset differences.
- Issue #5 date bounds are inclusive. Accept timezone-naive ISO date/datetime strings or Python date/datetime values, interpreted in the stored timestamp's time basis; reject timezone-aware bounds rather than assuming a source timezone. A date-only bound means midnight at the beginning of that date.
- Issue #5 uses all physical Parquet columns when columns is omitted. An explicit columns selection is the exact output projection. Apply quality filtering to the selected measurement columns using their paired flags; include flag fields in the output only when explicitly selected or when using the default all-column projection. The Parquet dataset schema is authoritative for selectable columns; VARIABLES remains the measurement-only catalog.
- Core versus helpers: selection by site, location, time range, and quality belongs in core. Post-access pandas transformations, such as adding water years and seasonal summaries, belong under streamwq.helpers.pandas. A quality-flag normalizer used for read-time selection is core, not automatically a pandas helper.
- Optional pandas: core imports and access work with Arrow alone. The streamwq[pandas] extra is established by #3; extend its dependencies only as required by the helpers. Both native to_pandas conversion and pandas-specific helpers require pandas to be installed.
- Watersheds: expose get_watersheds with a site_ids selection and return Arrow geometry data while preserving the metadata needed to recover the coordinate reference system and geometry. The exact geometry encoding and its compatibility requirements must be verified during implementation.
- Conversion: rely on native Arrow conversion methods for ordinary conversions. Expose streamwq.helpers.to_geopandas for geometry-aware conversion, restoring geometry and coordinate reference system information. GeoPandas is optional and loaded only when needed.
- Catalog: streamwq.catalog.DATASETS is a tuple of dataset names; VARIABLES is a read-only mapping from dataset names to measurement-variable tuples. Named variable constants remain available. Empty tuples for metadata, static_catchment_characteristics, watersheds, and huc_lookup mean no enumerated measurement list, not no columns. Storage paths and connection details are private. VARIABLES is not a complete column schema: #5 settled that selectable columns are validated against the physical Parquet schema and can be inspected on returned tables, without silently redefining VARIABLES.
- Breaking migration: remove old stream_utils imports and old pandas-returning reader interfaces rather than add a compatibility layer. Update maintained examples, package metadata, documentation, and tests affected by the rename and return contract.
- Scope discipline: reorganize affected helpers and public access surfaces without unrelated analysis rewrites. The precise placement of existing join, export, and interactive map capabilities was not decided in the conversation.

## Testing Decisions

- Status: the user explicitly confirmed the metadata/catalog public behavior boundary with local fixtures and fresh-process import/installation checks during #3. Other proposed boundaries below are not automatically approved by that confirmation.
- Prefer one primary test interface: the public module-level streamwq functions. Exercise the real access and selection implementation against small deterministic local Arrow/Parquet fixtures, substituting remote storage only at the existing internal storage-provider integration point.
- Good tests assert externally observable tables, schemas, selected identifiers, spatial metadata, and errors. Avoid coupling the acceptance suite to private helper calls, reader delegation, or the exact construction of Arrow expressions.
- Existing access tests provide prior art for dataset projections, date-range validation, required site selection, quality filters, injected storage providers, and watershed selection. Existing client tests cover metadata discovery, dataset availability, HUCs, bounding boxes, radii, and combined filters. Retain these behavioral cases while moving acceptance assertions to the new public interface.
- Existing site and spatial tests provide cases for identifier selection and geographic calculations. Existing temporal tests provide prior art for pandas-specific helper behavior. Existing catalog tests provide prior art for dataset discovery, with storage-path checks kept internal rather than made public requirements.
- Verify all four standard access functions return pyarrow.Table, including empty observation results. Verify get_sites has only STREAM_ID and no duplicate IDs; ensure direct discovery-to-access composition works.
- Parameterize access checks over the supported tabular dataset families and the three accepted site-input forms. Cover omitted, null, empty, wholly unknown, and mixed known/unknown site selections, plus known sites absent from the selected dataset.
- Verify missing site information and no matching observations produce distinct outcomes. Verify unknown IDs cause all-or-nothing rejection and are identified in errors.
- Test spatial filters, AND across filters, and OR among alternatives within one filter using fixtures with intentionally overlapping and non-overlapping matches. Do not assume discovery proves observation availability.
- Test get_metadata with omitted site_ids, a selected subset, and an explicitly empty selection. Verify get_data never interprets an omitted or empty selection as all sites.
- Carry forward #3's local Parquet fixtures and dependency-isolation checks. #4 must test real get_sites-to-get_metadata composition using the discovery table unchanged. #5 must separately validate identities against metadata before observation reads; permissive metadata filtering alone cannot establish that every requested ID exists. Cover shared authentication across mixed module-level reads as those APIs land. Expand focused typechecking to newly implemented public APIs.
- Test shared filters for positive selection behavior, unknown datasets and columns, invalid date ranges, and rejection of unsupported dataset/filter combinations. Preserve projected schemas for empty results.
- Test watershed-to-GeoPandas conversion through the public conversion helper, including geometry values and coordinate reference system preservation. Use the helper as a separate public test interface only because it is independently user-facing.
- Test pandas-specific analysis through its public helper functions. Use isolated installation or subprocess checks to verify core imports and local-fixture access work without pandas or GeoPandas; monkeypatching a dataframe conversion alone does not establish dependency independence.
- Verify package installation, new imports, catalog discoverability, optional extras, and maintained examples affected by the breaking migration. Avoid live HydroShare/S3 access as a requirement for the deterministic test suite.

## Out of Scope

- Implementing the migration as part of writing or publishing this specification.
- Backward-compatible imports, deprecated reader wrappers, or a transition period retaining pandas-returning access.
- Lazy, streaming, asynchronous, or out-of-core return interfaces.
- A pandas conversion wrapper that merely duplicates Arrow's native to_pandas method.
- Conversion adapters for unspecified additional frameworks.
- Changing source datasets, renaming stored STREAM_ID fields, or redesigning the HydroShare storage layout.
- Proving observation availability by scanning observations during get_sites.
- Replacing all existing scientific analysis implementations or redesigning interactive mapping tools.
- New join/export interfaces, an advanced configurable public client, or changes to supported Python versions without a separate requirement.
- Creating a glossary or architecture decision record as part of this handoff unless separately requested; this specification preserves the decisions in the meantime.

## Further Notes

### Domain Terms

- Site: a monitoring location identified in STREAM site metadata by STREAM_ID; it need not have observations in every dataset.
- Site metadata: descriptive information about sites, including information used to discover candidate sites.
- Site selection: one or more site identifiers supplied to restrict an access request.
- Candidate site: a site matching metadata-based discovery criteria, without a guarantee of observations matching a subsequent data request.
- Observation: a recorded measurement associated with a site and, for time-dependent datasets, a time.
- Watershed: the catchment geometry associated with a site, together with its descriptive attributes and spatial reference information.

### Current State and Resume Guidance

- Issues #3 and #4 are implemented and PRs #10 and #11 are merged into refine-examples. Issue #5 adds module-level get_data for water_quality and streamflow. The distribution, import package, and environment references are streamwq; optional pandas support remains, and no stream_utils compatibility package is provided. Python 3.14 and Arrow 25 support is unchanged.
- Remaining tabular observation readers and analysis helpers still use legacy interfaces under the renamed package. Maintained tutorials were mechanically renamed, not fully migrated. #6 adds the other tabular datasets; #9 owns completion of the breaking migration and maintained examples.
- Current spatial selection and temporal analysis consume pandas metadata or dataframes. Spatial selection remains core in the new design; pandas-specific temporal analysis moves to helpers.
- The package currently targets Python 3.14 and Arrow 25 through project/environment configuration. Preserve the existing supported versions unless a concrete compatibility problem requires an explicit follow-up decision.
- The conversation settled the contracts above, not every implementation detail. Before coding, inspect the live repository and preserve unrelated work. Do not interpret proposed testing details as independently approved architecture.
- Remaining details include geometry encoding, exact spatial argument names, observation identifier ordering, optional-extra spelling for GeoPandas, and missing optional-dependency messages. Watershed unknown-ID handling is explicitly unresolved: do not silently inherit metadata's permissive filtering or get_data's strict rejection. Follow compatible existing behavior where it does not conflict with an agreed decision; flag material ambiguity rather than inventing user approval.
- The one-column discovery contract deliberately does not expose the distance_km column currently added by some spatial discovery paths. Preserve identifier-only output; any new distance-reporting interface would need a separate decision.
- Package-name availability on a public registry was not verified. The chosen name is streamwq; do not silently substitute another name.
- Resume from the merged #3/#4 implementation and remaining issues #5-#9. Local Arrow/Parquet fixtures, lightweight import checks, installation checks, and documented examples provide working prior art; they do not establish completion of the remaining access APIs.