# Collibra Bulk Exporter to Neo4j

A powerful Python tool for bulk exporting metadata assets from Collibra Data Intelligence Cloud to Neo4j, enabling you to visualize and analyze your data governance knowledge graph.

## Overview

The Collibra Bulk Exporter seamlessly extracts assets, attributes, relations, and responsibilities from your Collibra instance and loads them into Neo4j. This enables powerful graph-based analytics and visualization of your metadata landscape.

### Key Benefits

- **Comprehensive Export**: Extract complete asset hierarchies with all metadata
- **Graph Visualization**: Transform tabular metadata into interconnected knowledge graphs  
- **High Performance**: Parallel processing and automatic pagination for large datasets
- **Production Ready**: Robust error handling, logging, and monitoring capabilities

## Features

-  **Bulk Asset Export** - Export assets by type ID with complete metadata
-  **Relationship Mapping** - Preserve all asset relationships and lineage
-  **Parallel Processing** - Multi-threaded execution for optimal performance
-  **Neo4j Integration** - Direct loading into Neo4j graph database
-  **OAuth Authentication** - Secure connection to Collibra instances
-  **Comprehensive Logging** - Detailed audit trails and debugging information
-  **Automatic Pagination** - Handle large datasets efficiently

## Architecture

```
collibra-bulk-exporter/
├── 📁 config/                      # Configuration management
│   └── Collibra_Asset_Type_Id_Manager.json
├── 📁 logs/                        # Application logs
├── 📁 outputs/                     # Exported data files
├── 📁 src/                         # Source code
│   ├── 📁 collibra_exporter/       # Main package
│   │   ├── 📁 api/                 # API layer
│   │   │   ├── fetcher.py          # Data fetching engine
│   │   │   ├── graphql_query.py    # Query generation
│   │   │   └── oauth_auth.py       # Authentication handler
│   │   ├── 📁 models/              # Data models
│   │   │   ├── exporter.py         # Export orchestration
│   │   │   └── transformer.py      # Data transformation
│   │   ├── 📁 utils/               # Utilities
│   │   │   ├── asset_type.py       # Asset type management
│   │   │   └── logging_config.py   # Logging setup
│   │   ├── processor.py            # Core processing
│   │   └── __init__.py             # Package init
│   └── main.py                     # Application entry point
├── .env                            # Environment configuration
├── requirements.txt                # Python dependencies
└── README.md                       # This file
```

##  Quick Start

### Prerequisites

- Python 3.8 or higher
- Neo4j database (local or cloud instance)
- Collibra Data Intelligence Cloud access
- OAuth application registered in Collibra

### Installation

1. **Clone the Repository**
   ```bash
   git clone https://github.com/sayan123234/Collibra-Bulk-Exporter-To-Neo4j.git
   cd Collibra-Bulk-Exporter-To-Neo4j
   ```

2. **Set Up Virtual Environment**
   ```bash
   # Create virtual environment
   python -m venv venv
   
   # Activate virtual environment
   # Windows
   venv\Scripts\activate
   # macOS/Linux  
   source venv/bin/activate
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## ⚙️ Configuration

### 1. Collibra OAuth Setup

Configure OAuth authentication in your Collibra instance:

1. **Access OAuth Settings**
   - Navigate to **Settings** → **OAuth Applications** in Collibra
   
2. **Register New Application**
   - Click **Register Application**
   - Set integration type to **"Integration"**
   - Provide a descriptive application name
   
3. **Save Credentials**
   - Copy the generated `clientId` and `clientSecret`
   - These will be used in your `.env` file

### 2. Environment Configuration

Create a `.env` file in the project root:

```env
# Collibra Configuration
COLLIBRA_INSTANCE_URL=your-instance.collibra.com
CLIENT_ID=your_oauth_client_id
CLIENT_SECRET=your_oauth_client_secret

# Neo4j Configuration  
NEO4J_URI=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_neo4j_password
NEO4J_DATABASE=neo4j
```

### 3. Asset Type Configuration

Update `config/Collibra_Asset_Type_Id_Manager.json` with your target asset types:

```json
{
    "ids": [
        "00000000-0000-0000-0000-000000031008",  // Table
        "00000000-0000-0000-0000-000000031007",  // Column
        "00000000-0000-0000-0000-000000000314"   // Business Term
    ]
}
```

> 💡 **Tip**: Find asset type IDs in Collibra Console under **Settings** → **Model** → **Asset Types**

## 🎯 Usage

### Basic Export

```bash
python src/main.py
```

### Advanced Usage Examples

**Export specific asset types:**
```bash
# Modify config file to include only desired asset type IDs
python src/main.py
```

**Monitor progress:**
```bash
# Logs are automatically generated in logs/ directory
tail -f logs/collibra_exporter.log
```

## 📊 Output

The tool generates:

- **Neo4j Graph**: Assets loaded as nodes with relationships preserved
- **Log Files**: Detailed execution logs in `logs/` directory  
- **Export Files**: Raw data exports in `outputs/` directory (if configured)

### Sample Neo4j Queries

After export, explore your data with Cypher queries:

```cypher
// View all asset types
MATCH (n) RETURN DISTINCT labels(n) as AssetTypes

// Find tables and their columns
MATCH (table:Table)-[:HAS_COLUMN]->(column:Column) 
RETURN table.name, collect(column.name) as columns

// Discover data lineage
MATCH path = (source)-[:FEEDS_INTO*]->(target)
RETURN path LIMIT 10
```

## Troubleshooting

### Common Issues

**Authentication Errors**
```
Error: OAuth authentication failed
```
- Verify `CLIENT_ID` and `CLIENT_SECRET` in `.env`
- Ensure OAuth application has proper permissions
- Check `COLLIBRA_INSTANCE_URL` format (no https:// prefix)

**Connection Issues**
```
Error: Cannot connect to Neo4j
```
- Verify Neo4j is running and accessible
- Check `NEO4J_URI`, `NEO4J_USERNAME`, and `NEO4J_PASSWORD`
- Test connection using Neo4j Browser

**Configuration Errors**
```
Error: Invalid asset type ID
```
- Verify asset type IDs exist in your Collibra instance
- Check JSON syntax in configuration file
- Ensure IDs are in correct UUID format

### Performance Optimization

- **Large Datasets**: Increase batch sizes in configuration
- **Memory Issues**: Reduce parallel processing threads
- **Network Timeouts**: Adjust timeout settings in OAuth configuration

## 📝 Logging

Logs are generated in `logs/collibra_exporter.log` with different levels:

- **INFO**: General execution progress
- **DEBUG**: Detailed processing information  
- **WARNING**: Non-critical issues
- **ERROR**: Critical failures requiring attention

##  Contributing

We welcome contributions! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request


##  Support

- **Issues**: [GitHub Issues](https://github.com/sayan123234/Collibra-Bulk-Exporter-To-Neo4j/issues)
- **Documentation**: [Wiki](https://github.com/sayan123234/Collibra-Bulk-Exporter-To-Neo4j/wiki)
- **Discussions**: [GitHub Discussions](https://github.com/sayan123234/Collibra-Bulk-Exporter-To-Neo4j/discussions)

##  Acknowledgments

- Collibra for providing comprehensive metadata management capabilities
- Neo4j for powerful graph database technology
- The open-source community for invaluable tools and libraries

---

**Made with ❤️ for the data governance community**