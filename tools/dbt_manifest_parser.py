import json
import sqlite3
from pathlib import Path
from datetime import datetime

def create_tables(conn):
    cursor = conn.cursor()
    
    # Create all the necessary tables
    # Metadata table
    cursor.execute("""
    CREATE TABLE metadata (
        dbt_schema_version TEXT,
        dbt_version TEXT,
        generated_at TEXT,
        invocation_id TEXT,
        env TEXT,
        project_name TEXT,
        project_id TEXT,
        user_id TEXT,
        send_anonymous_usage_stats BOOLEAN,
        adapter_type TEXT
    )
    """)
    
    # Nodes table
    cursor.execute("""
    CREATE TABLE nodes (
        unique_id TEXT PRIMARY KEY,
        resource_type TEXT,
        package_name TEXT,
        path TEXT,
        original_file_path TEXT,
        name TEXT,
        description TEXT,
        raw_code TEXT,
        compiled_code TEXT,
        config TEXT,
        fqn TEXT,
        tags TEXT,
        meta TEXT,
        database TEXT,
        schema TEXT,
        alias TEXT
    )
    """)
    
    # Sources table
    cursor.execute("""
    CREATE TABLE sources (
        unique_id TEXT PRIMARY KEY,
        package_name TEXT,
        path TEXT,
        original_file_path TEXT,
        name TEXT,
        source_name TEXT,
        source_description TEXT,
        loader TEXT,
        identifier TEXT,
        resource_type TEXT,
        quoting TEXT,
        loaded_at_field TEXT,
        freshness TEXT,
        external TEXT,
        description TEXT,
        meta TEXT,
        source_meta TEXT,
        tags TEXT,
        config TEXT,
        fqn TEXT
    )
    """)
    
    # Macros table
    cursor.execute("""
    CREATE TABLE macros (
        unique_id TEXT PRIMARY KEY,
        package_name TEXT,
        path TEXT,
        original_file_path TEXT,
        name TEXT,
        macro_sql TEXT,
        resource_type TEXT,
        tags TEXT,
        meta TEXT,
        description TEXT,
        depends_on TEXT
    )
    """)
    
    # Exposures table
    cursor.execute("""
    CREATE TABLE exposures (
        unique_id TEXT PRIMARY KEY,
        package_name TEXT,
        path TEXT,
        original_file_path TEXT,
        name TEXT,
        type TEXT,
        owner TEXT,
        description TEXT,
        maturity TEXT,
        meta TEXT,
        tags TEXT,
        url TEXT,
        depends_on TEXT,
        config TEXT,
        fqn TEXT
    )
    """)
    
    # Metrics table
    cursor.execute("""
    CREATE TABLE metrics (
        unique_id TEXT PRIMARY KEY,
        package_name TEXT,
        path TEXT,
        original_file_path TEXT,
        name TEXT,
        description TEXT,
        label TEXT,
        calculation_method TEXT,
        expression TEXT,
        timestamp TEXT,
        time_grains TEXT,
        dimensions TEXT,
        filters TEXT,
        meta TEXT,
        tags TEXT,
        fqn TEXT,
        depends_on TEXT,
        config TEXT
    )
    """)
    
    # Docs table
    cursor.execute("""
    CREATE TABLE docs (
        unique_id TEXT PRIMARY KEY,
        package_name TEXT,
        path TEXT,
        original_file_path TEXT,
        name TEXT,
        block_contents TEXT,
        resource_type TEXT
    )
    """)

    # Groups table
    cursor.execute("""
    CREATE TABLE groups (
        unique_id TEXT PRIMARY KEY,
        package_name TEXT,
        path TEXT,
        original_file_path TEXT,
        name TEXT,
        owner TEXT
    )
    """)

    # Selectors table
    cursor.execute("""
    CREATE TABLE selectors (
        name TEXT PRIMARY KEY,
        definition TEXT
    )
    """)

    # Disabled table
    cursor.execute("""
    CREATE TABLE disabled (
        unique_id TEXT PRIMARY KEY,
        original_file_path TEXT,
        resource_type TEXT
    )
    """)

    # Parent_Map and Child_Map tables
    for map_type in ['parent_map', 'child_map']:
        cursor.execute(f"""
        CREATE TABLE {map_type} (
            unique_id TEXT,
            related_unique_id TEXT,
            PRIMARY KEY (unique_id, related_unique_id)
        )
        """)

    # Group_Map table
    cursor.execute("""
    CREATE TABLE group_map (
        group_unique_id TEXT,
        node_unique_id TEXT,
        PRIMARY KEY (group_unique_id, node_unique_id)
    )
    """)

    # Saved_Queries table
    cursor.execute("""
    CREATE TABLE saved_queries (
        name TEXT PRIMARY KEY,
        definition TEXT
    )
    """)

    # Semantic_Models table
    cursor.execute("""
    CREATE TABLE semantic_models (
        unique_id TEXT PRIMARY KEY,
        package_name TEXT,
        path TEXT,
        original_file_path TEXT,
        name TEXT,
        description TEXT,
        model TEXT,
        entities TEXT,
        dimensions TEXT,
        measures TEXT,
        defaults TEXT,
        meta TEXT,
        tags TEXT,
        fqn TEXT
    )
    """)

    # Unit_tests table
    cursor.execute("""
    CREATE TABLE unit_tests (
        unique_id TEXT PRIMARY KEY,
        package_name TEXT,
        path TEXT,
        original_file_path TEXT,
        name TEXT,
        description TEXT,
        model TEXT,
        test_type TEXT,
        input TEXT,
        expected TEXT,
        config TEXT,
        fqn TEXT
    )
    """)
    
    conn.commit()

def insert_metadata(conn, metadata):
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO metadata VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        metadata.get('dbt_schema_version'),
        metadata.get('dbt_version'),
        metadata.get('generated_at'),
        metadata.get('invocation_id'),
        json.dumps(metadata.get('env')),
        metadata.get('project_name'),
        metadata.get('project_id'),
        metadata.get('user_id'),
        metadata.get('send_anonymous_usage_stats'),
        metadata.get('adapter_type')
    ))
    conn.commit()

def insert_nodes(conn, nodes):
    cursor = conn.cursor()
    for node_id, node in nodes.items():
        cursor.execute("""
        INSERT INTO nodes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            node_id,
            node.get('resource_type'),
            node.get('package_name'),
            node.get('path'),
            node.get('original_file_path'),
            node.get('name'),
            node.get('description'),
            node.get('raw_code'),
            node.get('compiled_code'),
            json.dumps(node.get('config')),
            json.dumps(node.get('fqn')),
            json.dumps(node.get('tags')),
            json.dumps(node.get('meta')),
            node.get('database'),
            node.get('schema'),
            node.get('alias')
        ))
    conn.commit()

def insert_sources(conn, sources):
    cursor = conn.cursor()
    for source_id, source in sources.items():
        cursor.execute("""
        INSERT INTO sources VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            source_id,
            source.get('package_name'),
            source.get('path'),
            source.get('original_file_path'),
            source.get('name'),
            source.get('source_name'),
            source.get('source_description'),
            source.get('loader'),
            source.get('identifier'),
            source.get('resource_type'),
            json.dumps(source.get('quoting')),
            source.get('loaded_at_field'),
            json.dumps(source.get('freshness')),
            json.dumps(source.get('external')),
            source.get('description'),
            json.dumps(source.get('meta')),
            json.dumps(source.get('source_meta')),
            json.dumps(source.get('tags')),
            json.dumps(source.get('config')),
            json.dumps(source.get('fqn'))
        ))
    conn.commit()

def insert_macros(conn, macros):
    cursor = conn.cursor()
    for macro_id, macro in macros.items():
        cursor.execute("""
        INSERT INTO macros VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            macro_id,
            macro.get('package_name'),
            macro.get('path'),
            macro.get('original_file_path'),
            macro.get('name'),
            macro.get('macro_sql'),
            macro.get('resource_type'),
            json.dumps(macro.get('tags')),
            json.dumps(macro.get('meta')),
            macro.get('description'),
            json.dumps(macro.get('depends_on'))
        ))
    conn.commit()

def insert_docs(conn, docs):
    cursor = conn.cursor()
    for doc_id, doc in docs.items():
        cursor.execute("""
        INSERT INTO docs VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            doc_id,
            doc.get('package_name'),
            doc.get('path'),
            doc.get('original_file_path'),
            doc.get('name'),
            doc.get('block_contents'),
            doc.get('resource_type')
        ))
    conn.commit()

def insert_exposures(conn, exposures):
    cursor = conn.cursor()
    for exposure_id, exposure in exposures.items():
        cursor.execute("""
        INSERT INTO exposures VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            exposure_id,
            exposure.get('package_name'),
            exposure.get('path'),
            exposure.get('original_file_path'),
            exposure.get('name'),
            exposure.get('type'),
            json.dumps(exposure.get('owner')),
            exposure.get('description'),
            exposure.get('maturity'),
            json.dumps(exposure.get('meta')),
            json.dumps(exposure.get('tags')),
            exposure.get('url'),
            json.dumps(exposure.get('depends_on')),
            json.dumps(exposure.get('config')),
            json.dumps(exposure.get('fqn'))
        ))
    conn.commit()

def insert_metrics(conn, metrics):
    cursor = conn.cursor()
    for metric_id, metric in metrics.items():
        cursor.execute("""
        INSERT INTO metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            metric_id,
            metric.get('package_name'),
            metric.get('path'),
            metric.get('original_file_path'),
            metric.get('name'),
            metric.get('description'),
            metric.get('label'),
            metric.get('calculation_method'),
            metric.get('expression'),
            metric.get('timestamp'),
            json.dumps(metric.get('time_grains')),
            json.dumps(metric.get('dimensions')),
            json.dumps(metric.get('filters')),
            json.dumps(metric.get('meta')),
            json.dumps(metric.get('tags')),
            json.dumps(metric.get('fqn')),
            json.dumps(metric.get('depends_on')),
            json.dumps(metric.get('config'))
        ))
    conn.commit()

def insert_groups(conn, groups):
    cursor = conn.cursor()
    for group_id, group in groups.items():
        cursor.execute("""
        INSERT INTO groups VALUES (?, ?, ?, ?, ?, ?)
        """, (
            group_id,
            group.get('package_name'),
            group.get('path'),
            group.get('original_file_path'),
            group.get('name'),
            json.dumps(group.get('owner'))
        ))
    conn.commit()

def insert_selectors(conn, selectors):
    cursor = conn.cursor()
    for name, definition in selectors.items():
        cursor.execute("""
        INSERT INTO selectors VALUES (?, ?)
        """, (name, json.dumps(definition)))
    conn.commit()

def insert_disabled(conn, disabled):
    cursor = conn.cursor()
    for unique_id, disabled_item in disabled.items():
        cursor.execute("""
        INSERT INTO disabled VALUES (?, ?, ?)
        """, (
            unique_id,
            disabled_item.get('original_file_path'),
            disabled_item.get('resource_type')
        ))
    conn.commit()

def insert_map(conn, map_data, table_name):
    cursor = conn.cursor()
    for unique_id, related_ids in map_data.items():
        for related_id in related_ids:
            cursor.execute(f"""
            INSERT INTO {table_name} VALUES (?, ?)
            """, (unique_id, related_id))
    conn.commit()

def insert_group_map(conn, group_map):
    cursor = conn.cursor()
    for group_id, node_ids in group_map.items():
        for node_id in node_ids:
            cursor.execute("""
            INSERT INTO group_map VALUES (?, ?)
            """, (group_id, node_id))
    conn.commit()

def insert_saved_queries(conn, saved_queries):
    cursor = conn.cursor()
    for name, definition in saved_queries.items():
        cursor.execute("""
        INSERT INTO saved_queries VALUES (?, ?)
        """, (name, json.dumps(definition)))
    conn.commit()

def insert_semantic_models(conn, semantic_models):
    cursor = conn.cursor()
    for model_id, model in semantic_models.items():
        cursor.execute("""
        INSERT INTO semantic_models VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            model_id,
            model.get('package_name'),
            model.get('path'),
            model.get('original_file_path'),
            model.get('name'),
            model.get('description'),
            model.get('model'),
            json.dumps(model.get('entities')),
            json.dumps(model.get('dimensions')),
            json.dumps(model.get('measures')),
            json.dumps(model.get('defaults')),
            json.dumps(model.get('meta')),
            json.dumps(model.get('tags')),
            json.dumps(model.get('fqn'))
        ))
    conn.commit()

def insert_unit_tests(conn, unit_tests):
    cursor = conn.cursor()
    for test_id, test in unit_tests.items():
        cursor.execute("""
        INSERT INTO unit_tests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            test_id,
            test.get('package_name'),
            test.get('path'),
            test.get('original_file_path'),
            test.get('name'),
            test.get('description'),
            test.get('model'),
            test.get('test_type'),
            json.dumps(test.get('input')),
            json.dumps(test.get('expected')),
            json.dumps(test.get('config')),
            json.dumps(test.get('fqn'))
        ))
    conn.commit()

def parse_manifest(manifest_path, root_dir):
    # Read the manifest file
    with open(manifest_path, 'r') as f:
        manifest = json.load(f)
    
    # Get the project name from the manifest
    project_name = manifest['metadata'].get('project_name', 'unknown_project')
    
    # Generate the database filename
    date_str = datetime.now().strftime("%Y%m%d")
    db_filename = f"{project_name}__{date_str}.db"
    db_path = root_dir / db_filename
    
    # Create SQLite database and tables
    conn = sqlite3.connect(db_path)
    create_tables(conn)
    
    # Insert data into tables
    insert_metadata(conn, manifest['metadata'])
    insert_nodes(conn, manifest['nodes'])
    insert_sources(conn, manifest.get('sources', {}))
    insert_macros(conn, manifest.get('macros', {}))
    insert_docs(conn, manifest.get('docs', {}))
    insert_exposures(conn, manifest.get('exposures', {}))
    insert_metrics(conn, manifest.get('metrics', {}))
    insert_groups(conn, manifest.get('groups', {}))
    insert_selectors(conn, manifest.get('selectors', {}))
    insert_disabled(conn, manifest.get('disabled', {}))
    insert_map(conn, manifest.get('parent_map', {}), 'parent_map')
    insert_map(conn, manifest.get('child_map', {}), 'child_map')
    insert_group_map(conn, manifest.get('group_map', {}))
    insert_saved_queries(conn, manifest.get('saved_queries', {}))
    insert_semantic_models(conn, manifest.get('semantic_models', {}))
    insert_unit_tests(conn, manifest.get('unit_tests', {}))
    
    conn.close()
    return db_path

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python dbt_manifest_parser.py <path_to_manifest.json>")
        sys.exit(1)
    
    manifest_path = Path(sys.argv[1])
    root_dir = Path.cwd()
    
    try:
        db_path = parse_manifest(manifest_path, root_dir)
        print(f"Manifest parsed successfully. Database created at: {db_path}")
    except Exception as e:
        print(f"Error parsing manifest: {str(e)}")