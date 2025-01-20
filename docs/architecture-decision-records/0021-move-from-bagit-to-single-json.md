# 21. Architectural Decision Record (ADR): Moving from BagIt Package Format to Single Metadata JSON File

Supersedes [15. BagIt-like package for Generic Ingest](https://github.com/nationalarchives/dr2-ingest/blob/main/docs/architecture-decision-records/0015-bagit-like-package-for-generic-ingest.md)

## Context

The implementation of the Court Document ingest process into Preservica initially utilised a package format based on the BagIt specification. This decision was influenced by prior experience within the Digital Archiving domain, as BagIt is a widely recognised standard for packaging and describing content. The BagIt structure organises content and metadata in a predictable way, making it suitable for preserving digital artifacts.

However, the complexity of creating and parsing BagIt packages in our generic ingest workflow has introduced several challenges, including:
1. **Operational Overhead**: Generating the BagIt structure requires additional processing steps, including directory organisation and checksum calculation, which increase the complexity of our ingest pipeline.
2. **Parsing Complexity**: Extracting and processing metadata from the BagIt package involves navigating multiple files and formats, increasing the potential for errors and maintenance burden.
3. **Increased I/O and Storage Costs**: BagIt packages often duplicate metadata files within each batch, leading to inefficiencies in terms of storage and data transfer.

Given these issues, we revisited our approach to metadata management to explore alternatives that reduce complexity while maintaining the fidelity and reliability of our ingest process.

## Decision

We decided to replace the BagIt package format with a simplified structure consisting of the batch content alongside a single metadata JSON file.

### Key Aspects of the New Format
- **Single Metadata JSON File**: All relevant metadata describing the batch is consolidated into a single JSON file, eliminating the need for multiple metadata files or formats.
- **Simplified Structure**: The batch content and metadata JSON file are submitted together, significantly reducing the operational and parsing complexity of the ingest process.
- **Retention of Key Features**: The metadata file retains all essential descriptive and structural information previously stored in the BagIt format, ensuring no loss of functionality or fidelity.

## Rationale

The move to a single metadata JSON file addresses the challenges posed by the BagIt format:
1. **Reduction in Complexity**: Consolidating metadata into a single JSON file eliminates the need for directory organisation, multiple metadata files, and checksum files inherent in BagIt. This streamlines both the creation and parsing processes.
2. **Improved Maintainability**: JSON is a widely supported, human-readable format that simplifies validation, debugging, and future modifications.
3. **Efficiency Gains**: Removing the need for BagIt-specific file generation reduces I/O operations and storage requirements, leading to performance improvements in the ingest pipeline.
4. **Alignment with Modern Practices**: Using a single JSON file aligns with current best practices in system interoperability and API design, facilitating easier integration with other tools and systems.

## Consequences

### Positive Outcomes
- **Simplified Workflow**: Developers and operators now work with a straightforward structure, reducing the learning curve and risk of errors.
- **Performance Improvements**: Faster processing and reduced storage overhead contribute to overall efficiency.
- **Enhanced Interoperability**: JSON's compatibility with a wide range of tools and libraries supports broader integration and automation opportunities.

### Potential Drawbacks
- **Loss of BagIt Standard Compliance**: Transitioning away from BagIt means losing the inherent benefits of a standardised package format, such as compatibility with existing BagIt tools.
- **Migration Effort**: Existing processes relying on BagIt need to be updated to accommodate the new format, requiring an initial investment of time and resources.

## Alternatives Considered
1. **Optimising BagIt Implementation**: Enhancing our BagIt processing pipeline to reduce overhead and complexity. However, this would still retain the intrinsic challenges of the format.
2. **Hybrid Approach**: Retaining BagIt for some use cases while using a simplified format for others. This was deemed overly complex and inconsistent.

## Decision Outcome

The single metadata JSON file approach has been implemented and has successfully reduced complexity in the Court Document ingest process. Early results indicate improved performance and maintainability without sacrificing the fidelity of metadata management.
