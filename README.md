# Collibra Bulk Exporter to Neo4j

A Python tool that exports metadata assets from Collibra Data Intelligence Cloud to Neo4j graph database, enabling graph-based visualization and analysis of your data governance landscape.

## Overview

This tool extracts assets, attributes, relations, and responsibilities from Collibra using GraphQL APIs and loads them into Neo4j as interconnected nodes and relationships. It supports bulk export operations with configurable asset type IDs and provides performance optimization through caching and parallel processing.

## Features

- **Bulk Asset Export** - Export assets by configurable type IDs with complete metadata
- **GraphQL Integration** - Efficient data fetching using Collibra's GraphQL API
- **Neo4j Loading** - Direct import into Neo4j with proper node/relationship mapping
- **Performance Optimization** - Parallel processing, caching, and connection pooling
- **OAuth Authentication** - Secure connection to Collibra instances
- **Automatic Pagination** - Handle large datasets efficiently

## Installation & Setup

### Prerequisites

- Python 3.8+
- Neo4j database (local or cloud)
- Collibra Data Intelligence Cloud access with OAuth app

### Quick Setup

1. **Clone and Install**
   ```bash
   git clone https://github.com/sayan123234/Collibra-Bulk-Exporter-To-Neo4j.git
   cd Collibra-Bulk-Exporter-To-Neo4j
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configure OAuth in Collibra**
   - Go to **Settings** → **OAuth Applications** in Collibra
   - Register new application with type **"Integration"**
   - Save the generated `clientId` and `clientSecret`

3. **Environment Configuration**
   
   Create `.env` file:
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

4. **Configure Asset Types**
   
   Edit `config/Collibra_Asset_Type_Id_Manager.json` with target asset type IDs:

```json
{
    "ids": [
        "00000000-0000-0000-0000-000000031008",
        "00000000-0000-0000-0000-000000031007",
        "00000000-0000-0000-0000-000000000314"
    ]
}
```

> **Tip**: Find asset type IDs in Collibra Console under **Settings** → **Model** → **Asset Types**

## Usage

Run the export:
```bash
python src/main.py
```

Monitor progress:
```bash
tail -f logs/collibra_exporter.log
```

## Output

- **Neo4j Graph**: Assets as nodes with preserved relationships
- **Logs**: Execution details in `logs/collibra_exporter.log`

### Sample Neo4j Queries

```cypher
// View all asset types
MATCH (n) RETURN DISTINCT labels(n)

// Find relationships
MATCH (a)-[r]->(b) RETURN type(r), count(*) ORDER BY count(*) DESC
```

## Troubleshooting

**Authentication Issues**
- Verify `CLIENT_ID` and `CLIENT_SECRET` in `.env`
- Check `COLLIBRA_INSTANCE_URL` format (no https:// prefix)

**Neo4j Connection Issues**  
- Verify Neo4j is running and credentials are correct
- Test connection using Neo4j Browser

**Configuration Issues**
- Verify asset type IDs exist in Collibra
- Check JSON syntax in config file

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Open a Pull Request
