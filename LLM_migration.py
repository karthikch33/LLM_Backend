import os
import json
import re
import sqlite3
import pandas as pd
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold

# API Configuration
API_KEY = "AIzaSyCEMEumQOzsgxEZPGsp9-Erd0cQt-PNmbg"

project_id = 0
object_id = 0
segment_id = 0
user_prompt_from_backend = ''

if not API_KEY:
    raise ValueError("GEMINI_API_KEY environment variable not set")
genai.configure(api_key=API_KEY)

def gemini_call(prompt, model="gemini-2.0-flash"):
    """Call the Gemini API to generate a response for the given prompt."""
    try:
        safety_settings = {
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        }
        
        model = genai.GenerativeModel(
            model_name=model,
            generation_config={
                "temperature": 0.2,
                "max_output_tokens": 2048,
                "top_p": 0.95,
                "top_k": 40,
            },
            safety_settings=safety_settings
        )
        # print(prompt)
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"Error calling Gemini API: {e}")
        return None

# def setup_database():
#     """Set up a sample SQLite database with required tables for testing."""
#     conn = sqlite3.connect("C:\\Users\\varunbalaji.gada\\Desktop\\LLM_Project\\backend\\db.sqlite3")
#     cursor = conn.cursor()
    
#     # Drop existing tables to ensure a clean setup
#     cursor.execute("DROP TABLE IF EXISTS connection_segments;")
#     cursor.execute("DROP TABLE IF EXISTS connection_rule;")
#     cursor.execute("DROP TABLE IF EXISTS connection_fields;")
#     cursor.execute("DROP TABLE IF EXISTS KNMT;")
#     cursor.execute("DROP TABLE IF EXISTS KDMAT;")
#     cursor.execute("DROP TABLE IF EXISTS t_9_Product_General_Data_mandatory;")
    
#     # Create connection_segments table
#     cursor.execute("""
#         CREATE TABLE IF NOT EXISTS connection_segments (
#             project_id INTEGER,
#             object_id INTEGER,
#             segment_id INTEGER,
#             table_name TEXT,
#             ROWID INTEGER PRIMARY KEY
#         );
#     """)
#     cursor.execute("INSERT OR REPLACE INTO connection_segments (project_id, object_id, segment_id, table_name, ROWID) VALUES (8, 9,52, 't_9_Product_General_Data_mandatory', 1);")
    
#     # Create connection_rule table based on the screenshot
#     cursor.execute("""
#         CREATE TABLE IF NOT EXISTS connection_rule (
#             ROWID INTEGER PRIMARY KEY,
#             check_box INTEGER,
#             object_id INTEGER,
#             project_id INTEGER,
#             segment_id INTEGER,
#             data_mapping_type TEXT,
#             isMandatory INTEGER,
#             field_id INTEGER
#         );
#     """)
#     cursor.execute("INSERT OR REPLACE INTO connection_rule (ROWID, check_box, object_id, project_id, segment_id, data_mapping_type, isMandatory, field_id) VALUES (1, 0, 8, 52, 9, 'mapping', 1, 1);")
#     cursor.execute("INSERT OR REPLACE INTO connection_rule (ROWID, check_box, object_id, project_id, segment_id, data_mapping_type, isMandatory, field_id) VALUES (2, 0, 8, 52, 9, 'mapping', 1, 2);")
#     cursor.execute("INSERT OR REPLACE INTO connection_rule (ROWID, check_box, object_id, project_id, segment_id, data_mapping_type, isMandatory, field_id) VALUES (3, 0, 8, 52, 9, 'mapping', 1, 3);")
#     cursor.execute("INSERT OR REPLACE INTO connection_rule (ROWID, check_box, object_id, project_id, segment_id, data_mapping_type, isMandatory, field_id) VALUES (4, 0, 8, 52, 9, 'mapping', 1, 4);")
#     cursor.execute("INSERT OR REPLACE INTO connection_rule (ROWID, check_box, object_id, project_id, segment_id, data_mapping_type, isMandatory, field_id) VALUES (5, 0, 8, 52, 9, 'mapping', 1, 5);")
    
#     # Create connection_fields table for field mappings
#     cursor.execute("""
#         CREATE TABLE IF NOT EXISTS connection_fields (
#             field_id INTEGER PRIMARY KEY,
#             source_table TEXT,
#             source_field_name TEXT,
#             target_sap_table TEXT,
#             target_sap_field TEXT
#         );
#     """)
#     cursor.execute("INSERT OR REPLACE INTO connection_fields (field_id, source_table, source_field_name, target_sap_table, target_sap_field) VALUES (1, 'KNMT', 'VKORG', 't_9_Product_General_Data_mandatory', 'SALESORGANIZATION');")
#     cursor.execute("INSERT OR REPLACE INTO connection_fields (field_id, source_table, source_field_name, target_sap_table, target_sap_field) VALUES (2, 'KNMT', 'VTWEG', 't_9_Product_General_Data_mandatory', 'DISTRIBUTIONCHANNEL');")
#     cursor.execute("INSERT OR REPLACE INTO connection_fields (field_id, source_table, source_field_name, target_sap_table, target_sap_field) VALUES (3, 'KNMT', 'KUNNR', 't_9_Product_General_Data_mandatory', 'CUSTOMER');")
#     cursor.execute("INSERT OR REPLACE INTO connection_fields (field_id, source_table, source_field_name, target_sap_table, target_sap_field) VALUES (4, 'KNMT', 'MATNR', 't_9_Product_General_Data_mandatory', 'MATERIAL');")
#     cursor.execute("INSERT OR REPLACE INTO connection_fields (field_id, source_table, source_field_name, target_sap_table, target_sap_field) VALUES (5, 'KNMT', 'KDMAT', 't_9_Product_General_Data_mandatory', 'MATERIALBYCUSTOMER');")
    
#     # Create KNMT table
#     cursor.execute("""
#         CREATE TABLE IF NOT EXISTS KNMT (
#             ROWID INTEGER PRIMARY KEY,
#             VKORG TEXT,
#             VTWEG TEXT,
#             KUNNR TEXT,
#             MATNR TEXT,
#             KDMAT TEXT
#         );
#     """)
#     cursor.execute("INSERT OR REPLACE INTO KNMT (ROWID, VKORG, VTWEG, KUNNR, MATNR, KDMAT) VALUES (1, '1000', '10', 'CUST123', 'MAT456', 'MOBILE:144');")
    
#     # Create KDMAT table (for filtering)
#     cursor.execute("""
#         CREATE TABLE IF NOT EXISTS KDMAT (
#             KDMAT TEXT PRIMARY KEY
#         );
#     """)
#     cursor.execute("INSERT OR REPLACE INTO KDMAT (KDMAT) VALUES ('MOBILE',144');")
#     cursor.execute("INSERT OR REPLACE INTO KDMAT (KDMAT) VALUES ('AUDI Q7');")
    
#     # Create target table
#     cursor.execute("""
#         CREATE TABLE IF NOT EXISTS t_9_Product_General_Data_mandatory (
#             ROWID INTEGER PRIMARY KEY,
#             SALESORGANIZATION TEXT,
#             DISTRIBUTIONCHANNEL TEXT,
#             CUSTOMER TEXT,
#             MATERIAL TEXT,
#             MATERIALBYCUSTOMER TEXT
#         );
#     """)
#     cursor.execute("INSERT OR REPLACE INTO t_9_Product_General_Data_mandatory (ROWID, SALESORGANIZATION, DISTRIBUTIONCHANNEL, CUSTOMER, MATERIAL, MATERIALBYCUSTOMER) VALUES (1, '1000', '10', 'CUST123', 'MAT456', NULL);")
    
#     conn.commit()
#     # Debug: Print table schema to verify
#     cursor.execute("PRAGMA table_info(connection_rule);")
#     print("connection_rule schema:", cursor.fetchall())
#     conn.close()
#     return sqlite3.connect("C:\\Users\\varunbalaji.gada\\Desktop\\LLM_Project\\backend\\db.sqlite3")  # Reopen connection to ensure consistency

def get_table_metadata(conn, dmc_mappings, project_id, object_id, segment_id, sample_data=True, row_limit=5):
    """Get metadata for all tables with DMC mapping rules incorporated."""
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    metadata = {}
    for table in tables:
        table_name = table[0]
        if table_name.startswith('sqlite_'):
            continue
            
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        create_table_stmt = f"CREATE TABLE {table_name} (\n"
        for col in columns:
            create_table_stmt += f"    {col[1]} {col[2]}"
            if col[3]:
                create_table_stmt += " NOT NULL"
            if col[5]:
                create_table_stmt += " PRIMARY KEY"
            create_table_stmt += ",\n"
        create_table_stmt = create_table_stmt.rstrip(",\n") + "\n);"
        
        field_mappings = {}
        for mapping in dmc_mappings:
            if mapping["target_table"] == table_name:
                field_mappings[mapping["target_field"]] = {
                    "source_table": mapping["source_table"],
                    "source_field_name": mapping["source_field_name"],
                    "description": mapping["description"]
                }
        
        metadata[table_name] = {
            "schema": create_table_stmt,
            "columns": [col[1] for col in columns],
            "field_mappings": field_mappings
        }
        
        if sample_data:
            try:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT {row_limit};")
                sample_rows = cursor.fetchall()
                if sample_rows:
                    col_names = [col[1] for col in columns]
                    sample_data_str = "Sample data:\n| " + " | ".join(col_names) + " |\n| " + " | ".join(["---" for _ in col_names]) + " |\n"
                    for row in sample_rows:
                        sample_data_str += "| " + " | ".join([str(val) for val in row]) + " |\n"
                    metadata[table_name]["sample_data"] = sample_data_str
            except Exception as e:
                print(f"Error fetching sample data for {table_name}: {e}")
    
    return metadata

def load_dmc_mappings(dmc_path, project_id, object_id, segment_id):
    """Load DMC mapping rules from either a SQLite database or an Excel file."""
    if dmc_path.endswith('.xlsx') or dmc_path.endswith('.xls'):
        try:
            # Load from Excel file
            df = pd.read_excel(dmc_path)
            mappings = []
            for _, row in df.iterrows():
                # Adjust column names based on your Excel structure
                mappings.append({
                    "project_id": project_id,
                    "object_id": object_id,
                    "segment_id": segment_id,
                    "source_table": row.get('source_table', ''),
                    "source_field_name": row.get('source_field_name', ''),
                    "target_table": row.get('target_sap_table', ''),
                    "target_field": row.get('target_sap_field', ''),
                    "description": f"{row.get('source_field_name', '')} mapping"
                })
            return mappings
        except Exception as e:
            print(f"Error loading Excel file: {e}")
            return []
    else:
        # SQLite database loading logic
        conn = sqlite3.connect(dmc_path)
        cursor = conn.cursor()


        cursor.execute("""
        SELECT table_name FROM connection_segments 
        WHERE segment_id = ? AND obj_id_id = ? AND project_id_id = ?
        """, (segment_id, object_id, project_id))
        field_ids = [row[0] for row in cursor.fetchall()]
        
        if not field_ids:
            cursor.close()
            conn.close()
            return []
        
        cursor.execute("""
            SELECT source_table, source_field_name, target_sap_table, target_sap_field
            FROM connection_rule
            WHERE field_id IN ({})
        """.format(','.join('?' * len(field_ids))), field_ids)
        
        mappings = []
        for row in cursor.fetchall():
            mappings.append({
                "project_id": project_id,
                "object_id": object_id,
                "segment_id": segment_id,
                "source_table": row[0],
                "source_field_name": row[1],
                "target_table": row[2],
                "target_field": row[3],
                "description": f"{row[1]} mapping"
            })
        
        cursor.close()
        conn.close()
        return mappings

def match_description_to_field(description, dmc_mappings):
    """Match a user description to an actual field name using DMC mappings."""
    description_lower = description.lower().strip()
    
    for mapping in dmc_mappings:
        if mapping["target_field"].lower() in description_lower or mapping["source_field_name"].lower() in description_lower:
            return {
                "target_table": mapping["target_table"],
                "target_field": mapping["target_field"],
                "source_table": mapping["source_table"],
                "source_field_name": mapping["source_field_name"]
            }
    
    best_match = None
    best_score = 0
    desc_words = set(description_lower.split())
    
    for mapping in dmc_mappings:
        mapping_words = set((mapping["target_field"] + " " + mapping["source_field_name"]).lower().split())
        common_words = desc_words.intersection(mapping_words)
        if common_words:
            score = len(common_words) / max(len(desc_words), len(mapping_words))
            if score > best_score:
                best_score = score
                best_match = mapping
    
    if best_match and best_score > 0.3:
        return {
            "target_table": best_match["target_table"],
            "target_field": best_match["target_field"],
            "source_table": best_match["source_table"],
            "source_field_name": best_match["source_field_name"]
        }
    
    return None

def parse_multiple_queries(response_text):
    """Extract multiple SQL queries from the LLM response."""
    queries = []
    
    if not response_text:
        return ["Error: Empty response"]
    
    try:
        # Try to parse as JSON first
        if "{" in response_text and "}" in response_text:
            json_pattern = r'(\{.*?\})'
            json_matches = re.findall(json_pattern, response_text, re.DOTALL)
            
            for json_str in json_matches:
                try:
                    json_data = json.loads(json_str)
                    if "result" in json_data:
                        queries.append(clean_query(json_data["result"]))
                    elif "queries" in json_data:
                        for q in json_data["queries"]:
                            queries.append(clean_query(q))
                    elif "sql" in json_data:
                        queries.append(clean_query(json_data["sql"]))
                except json.JSONDecodeError:
                    continue
        
        # If no valid JSON found, try extracting SQL code blocks
        if not queries:
            sql_blocks = re.findall(r'```(?:sql)?\s*(.*?)\s*```', response_text, re.DOTALL)
            for block in sql_blocks:
                for query in re.split(r';\s*', block):
                    if query.strip():
                        queries.append(clean_query(query))
        
        # If still no queries, try to extract SQL statements directly
        if not queries:
            sql_pattern = r'((?:SELECT|INSERT|UPDATE|DELETE).*?(?:;|$))'
            sql_matches = re.findall(sql_pattern, response_text, re.IGNORECASE | re.DOTALL)
            for match in sql_matches:
                queries.append(clean_query(match))
    except Exception as e:
        print(f"Error parsing response: {e}")
    
    # If still no queries found, return the full response
    if not queries:
        return ["-- Could not parse query. Raw response: " + response_text]
    
    return queries

def clean_query(query):
    """Clean up a SQL query by removing ellipses and fixing common issues."""
    query = re.sub(r'\.{3}', '', query)
    query = re.sub(r',\s*,', ',', query)
    query = re.sub(r',\s*FROM', ' FROM', query)
    
    # For SQLite UPDATE queries with FROM clause
    if query.strip().upper().startswith("UPDATE") and " FROM " in query.upper():
        # Extract the components of the UPDATE query
        update_match = re.search(r'UPDATE\s+(\w+)\s+SET\s+(.*?)\s+FROM\s+(\w+)\s+WHERE\s+(.*?)(?:;|$)', 
                                query, re.DOTALL | re.IGNORECASE)
        
        if update_match:
            target_table = update_match.group(1)
            set_clause = update_match.group(2)
            source_table = update_match.group(3)
            where_clause = update_match.group(4)
            
            # Rebuild the query for SQLite compatibility
            query = f"""UPDATE {target_table}
                       SET {set_clause}
                       WHERE EXISTS (SELECT 1 FROM {source_table} WHERE {where_clause});"""
    
    # Handle missing semicolon
    if not query.strip().endswith(';'):
        query = query.strip() + ';'
    
    return query

def detect_relevant_tables(user_query, metadata):
    """Detect which tables are relevant to a user query."""
    table_list = list(metadata.keys())
    
    # First, check for explicit table mentions
    mentioned_tables = []
    for table in table_list:
        # Check for table name mentions with various patterns
        patterns = [
            r'\b' + re.escape(table) + r'\b',  # Exact match
            r'\b' + re.escape(table.lower()) + r'\b',  # Lowercase match
            r'\b' + re.escape(table.upper()) + r'\b',  # Uppercase match
            r'from\s+' + re.escape(table) + r'\b',  # SELECT FROM pattern
            r'table\s+' + re.escape(table) + r'\b',  # Table keyword pattern
            r'\b' + re.escape(table) + r'\s+table\b',  # Table keyword pattern
        ]
        
        for pattern in patterns:
            if re.search(pattern, user_query, re.IGNORECASE):
                mentioned_tables.append(table)
                break
    
    if mentioned_tables:
        return mentioned_tables
    
    # If no explicit mentions, use Gemini to detect
    tables_prompt = f"""Identify which tables are most likely needed for this user query.

User query: "{user_query}"

Available tables: {', '.join(table_list)}

Respond with a JSON array of relevant table names, or an empty array if uncertain.
Example: ["KNMT", "t_9_Product_General_Data_mandatory"] or []
"""
    
    response = gemini_call(tables_prompt)
    
    if not response:
        return table_list[:5]
    
    try:
        match = re.search(r'\[.*?\]', response, re.DOTALL)
        if match:
            relevant_tables = json.loads(match.group(0))
            if relevant_tables and isinstance(relevant_tables, list):
                return relevant_tables
    except Exception as e:
        print(f"Error parsing relevant tables: {e}")
    
    # Default to first 5 tables if no tables detected
    return table_list[:5]

def generate_sql_query(user_query, conn, project_id, object_id, segment_id, dmc_path, include_sample_data=True):
    """Generate SQL query based on user input, database schema, and DMC mappings."""
    # Get target table from connection_segments
    cursor = conn.cursor()

    cursor.execute("""
    SELECT table_name FROM connection_segments 
    WHERE segment_id = ? AND obj_id_id = ? AND project_id_id = ?
    """, (segment_id, object_id, project_id))
    
    target_table = cursor.fetchone()
    if not target_table:
        return ["Error: No target table found in connection_segments"]
    target_table = target_table[0]

    # Check if user specified a source table
    user_specified_table = None
    # Look for common patterns indicating a table reference
    table_patterns = [
        r'from\s+(\w+)',
        r'table\s+(\w+)',
        r'(\w+)\s+table',
        r'in\s+(\w+)',
        r'(\w+)\.(\w+)',  # Matches table.column pattern
    ]
    
    for pattern in table_patterns:
        matches = re.findall(pattern, user_query, re.IGNORECASE)
        if matches:
            for match in matches:
                if isinstance(match, tuple):  # For patterns with groups
                    potential_table = match[0]
                else:
                    potential_table = match
                
                # Check if this is an actual table in the database
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (potential_table,))
                if cursor.fetchone():
                    user_specified_table = potential_table
                    break
            if user_specified_table:
                break
    
    # Use user-specified table or default to KNMT
    source_table = user_specified_table if user_specified_table else "KNMT"

    dmc_mappings = load_dmc_mappings(dmc_path, project_id, object_id, segment_id)
    metadata = get_table_metadata(conn, dmc_mappings, project_id, object_id, segment_id, sample_data=include_sample_data)

    field_extraction_prompt = f"""
    Extract all potential field descriptions from this user query about SAP data:
    
    "{user_query}"
    
    Return only a JSON array of the descriptions.
    Example: ["Material Number Used by Customer"]
    """
    
    field_descriptions_response = gemini_call(field_extraction_prompt)
    field_descriptions = []
    
    try:
        match = re.search(r'\[.*?\]', field_descriptions_response, re.DOTALL)
        if match:
            field_descriptions = json.loads(match.group(0))
    except Exception as e:
        print(f"Error parsing field descriptions: {e}")
    
    field_mappings = {}
    for description in field_descriptions:
        mapping = match_description_to_field(description, dmc_mappings)
        if mapping:
            field_mappings[description] = mapping
    
    relevant_tables = set([target_table, "KNMT"])  # Include KNMT as source table
    if not relevant_tables:
        relevant_tables = set(list(metadata.keys())[:5])
    
    schema_info = ""
    for table in relevant_tables:
        if table in metadata:
            schema_info += f"\n\n{metadata[table]['schema']}"
            columns = metadata[table].get('columns', [])
            if columns:
                schema_info += f"\n\nColumns in {table}: {', '.join(columns)}"
            if 'field_mappings' in metadata[table]:
                mappings = metadata[table]['field_mappings']
                if mappings:
                    schema_info += "\n\nField mappings:"
                    for target_field, mapping_info in mappings.items():
                        schema_info += f"\n- {target_field} ('{mapping_info['description']}')"
                        if mapping_info['source_table'] and mapping_info['source_field_name']:
                            schema_info += f" maps from {mapping_info['source_table']}.{mapping_info['source_field_name']}"
            if include_sample_data and 'sample_data' in metadata[table]:
                schema_info += f"\n\n{metadata[table]['sample_data']}"
    
    mapping_dict = {mapping["description"]: {
        "target_table": mapping["target_table"],
        "target_field": mapping["target_field"],
        "source": f"{mapping['source_table']}.{mapping['source_field_name']}" if mapping["source_table"] and mapping["source_field_name"] else None
    } for mapping in dmc_mappings}

# In the prompt generation section:
    prompt = f"""You are an expert SQL generator for SQLite databases focusing on SAP data migration with DMC mapping rules.
Generate a complete, executable SQL query for this user request: "{user_query}"

PROJECT CONTEXT:
- Project ID: {project_id}
- Object ID: {object_id}
- Segment ID: {segment_id}
- Version ID: 1
- Is Mandatory: True
- Target Table: {target_table} (from connection_segments)
- Primary Source Table: {source_table}

IMPORTANT INSTRUCTION:
If the user has specified "MARA" or any other table in their query, use that table instead of the default KNMT table. 
The user has mentioned "{source_table}" in their query, so use this as the source table.

DATABASE SCHEMA:
{schema_info}

DMC FIELD MAPPINGS:
{json.dumps(mapping_dict, indent=2)}

QUERY REQUIREMENTS ANALYSIS:
1. Carefully analyze the user query to identify:
   - Main action (SELECT, UPDATE, INSERT, DELETE)
   - Target fields to be updated or retrieved
   - Filter conditions requested
   - Specific values mentioned
   - Identify all the languages abbreviated E,D or any other symbol in the source table if any and transform them as per the user query    


2. For field mappings:
   - Only use mappings that are applicable to the specified source table ({source_table})
   - Adapt field names to match those in the specified source table


CONDITIONAL LOGIC REQUIREMENTS:
1. For sequential table checks (e.g., "check table1 first, if not found check table2, then table3"):
   - Use a nested COALESCE or CASE structure
   - Prioritize the tables in the exact order specified by the user
   - Only retrieve data from subsequent tables if no data is found in previous tables

EXAMPLE IMPLEMENTATION:
For a request like "update material type if material number is in mara_500 bring MEINS from mara_500, else if in mara_700 bring from it, else get from mara":

UPDATE target_table
SET MEINS = (
    SELECT COALESCE(
        (SELECT m500.MEINS FROM mara_500 m500 WHERE m500.MATNR = target_table.MATERIAL AND m500.MEINS IS NOT NULL LIMIT 1),
        (SELECT m700.MEINS FROM mara_700 m700 WHERE m700.MATNR = target_table.MATERIAL AND m700.MEINS IS NOT NULL LIMIT 1),
        (SELECT m.MEINS FROM mara m WHERE m.MATNR = target_table.MATERIAL LIMIT 1)
    )
    WHERE EXISTS (
        SELECT 1 FROM mara_500 WHERE MATNR = target_table.MATERIAL
        UNION
        SELECT 1 FROM mara_700 WHERE MATNR = target_table.MATERIAL
        UNION
        SELECT 1 FROM mara WHERE MATNR = target_table.MATERIAL
    )
)
WHERE [additional conditions if any];


SQLITE SYNTAX REQUIREMENTS:
1. SQLite does NOT support UPDATE with FROM clause. Use one of these patterns:
   a. For simple updates: 
      UPDATE target_table SET column = value WHERE condition;
   
   b. For updates with joins:
      UPDATE target_table 
      SET column = (SELECT source_column FROM source_table WHERE join_condition)
      WHERE EXISTS (SELECT 1 FROM source_table WHERE join_condition);
   
   c. For updates with multiple conditions:
      UPDATE target_table
      SET column = source_table.column
      FROM source_table
      WHERE target_table.key = source_table.key;
	  
2. For filtering with IN conditions:
   - Use proper syntax: column IN ('value1', 'value2')
   - For KDMAT filters, use: KNMT.KDMAT IN ('MOBILE:144', 'AUDI Q7')

3. JOIN conditions must be rewritten as subquery conditions in SQLite.

FILTER HANDLING:
1. Only apply filters explicitly mentioned in the user query.
2. If user mentions "filtering KDMAT is in ('MOBILE',144,'AUDI Q7')", include this exact filter.
3. Parse all specific values mentioned in the query for filtering.

RESPONSE FORMAT:
Return a complete, executable SQLite query that correctly implements the user's intent.
Format the query with proper indentation and line breaks for readability.
No explanations or comments - only return the SQL query in JSON format.

Respond with JSON in this format: {{"result": "YOUR SQL QUERY HERE"}}
"""



    response = gemini_call(prompt)
    if response:
        queries = parse_multiple_queries(response)
        return queries
    return ["Error: No response from API"]


def execute_and_display_query(query, conn, max_rows=10, return_object=True):
    """Execute a SQL query and return results as an object."""
    result_object = {
        "success": False,
        "message": "",
        "data": None,
        "rows_affected": 0
    }
    
    try:
        if any(query.strip().upper().startswith(stmt) for stmt in ["INSERT", "UPDATE", "DELETE"]):
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            rows_affected = cursor.rowcount
            result_object["success"] = True
            result_object["message"] = f"Query executed successfully. {rows_affected} rows affected."
            result_object["rows_affected"] = rows_affected
            if query.strip().upper().startswith("INSERT") and "SELECT" in query.upper():
                where_clause = re.search(r'WHERE\s+(.*?);', query, re.IGNORECASE | re.DOTALL)
                if where_clause:
                    table_match = re.search(r'INSERT\s+INTO\s+(\w+)', query, re.IGNORECASE)
                    if table_match:
                        table_name = table_match.group(1)
                        select_query = f"SELECT * FROM {table_name} WHERE {where_clause.group(1)}"
                        inserted_data = pd.read_sql_query(select_query, conn)
                        result_object["data"] = inserted_data.to_dict('records')
        else:
            df = pd.read_sql_query(query, conn)
            result_object["success"] = True
            if df.empty:
                result_object["message"] = "Query executed successfully, but returned no results."
            else:
                result_object["message"] = f"Query returned {len(df)} rows."
                result_object["data"] = df.to_dict('records')
        
        if return_object:
            return result_object
        else:
            if result_object["data"]:
                df = pd.DataFrame(result_object["data"])
                if len(df) > max_rows:
                    return df.head(max_rows).to_string() + f"\n\n(Showing {max_rows} of {len(df)} rows)"
                return df.to_string()
            return result_object["message"]
    except Exception as e:
        result_object["message"] = f"Error executing query: {str(e)}"
        if return_object:
            return result_object
        return result_object["message"]

def validate_sql_query(query, conn):
    """Use Gemini to validate and potentially fix a SQL query."""
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"EXPLAIN QUERY PLAN {query}")
        return {"valid": True, "message": "Query is valid", "query": query}
    except sqlite3.Error as e:
        error_msg = str(e)
        fix_prompt = f"""This SQL query has an error. Please fix it.

Error message: {error_msg}

Query:
{query}

Return ONLY the fixed query without explanations, formatted as a JSON object with key "fixed_query".
Example: {{"fixed_query": "SELECT * FROM table WHERE column = 'value';"}}
"""
        
        response = gemini_call(fix_prompt)
        if response:
            json_match = re.search(r'\{.*"fixed_query":\s*"(.*?)"\s*\}', response, re.DOTALL)
            if json_match:
                fixed_query = json_match.group(1).replace('\\', '')
                return {
                    "valid": False,
                    "message": f"Original error: {error_msg}",
                    "query": fixed_query,
                    "fixed": True
                }
        return {"valid": False, "message": error_msg, "query": query}

def test_gemini_connection():
    """Test if the Gemini API connection is working."""
    try:
        response = gemini_call("Say 'Gemini API connection successful'")
        if response and "successful" in response.lower():
            print(f"API test result: {response}")
            return True
        print(f"API test failed with response: {response}")
        return False
    except Exception as e:
        print(f"API connection error: {e}")
        return False


def main():
    # Database connection
    conn = sqlite3.connect("C:\\Users\\Aditya.chalavadi\\Desktop\\DMC-LLM\\backend\\db.sqlite3")
    
    try:
        # Test Gemini connection
        if not test_gemini_connection():
            raise Exception("Gemini API connection failed")
     
        
        
        print("Id's Retrived are :",project_id," ",object_id," ",segment_id)
        print("Retrieved User Prompt: ",user_prompt_from_backend)
        # user_query   =  '''Bring Material Number with Material Type = ROH from MARA Table and insert into target table'''
        # user_query = '''update material type in target table if material number is present in MARA_500 bring material type from MARA_500 else if not present in MARA_500 than check for material number in MARA_700 bring from it else if not found in MARA_500 ,MARA_700 get the material type from MARA'''
        # user_query = '''Update MATKL in target table from MARA If MATKL in ( L002, ZMGRP1,202R ) then 'GENAI01'  ELSE IF  MATKL in ( '01','L001','ZSUK1RM1') then "GENAI02' ELSE IF MATKL in ( 'CH001','02') then "GenAI03' Else 'GenAINo' in MARA Table'''
        # user_query = '''update Plant field in the  target table by Joining Material from target table with Material from MARC segment and Bring Material and Plant field from MARC Table for the plants ( 1710, 9999 )'''
        # user_query = '''clear the data MTART in the target table'''
        
        
        # project_id, object_id, segment_id = 15,24,264
        
        dmc_path = "C:\\Users\\Aditya.chalavadi\\Desktop\\DMC-LLM\\backend\\db.sqlite3"
        
        # Generated SQL query
        queries = generate_sql_query(user_prompt_from_backend, conn, project_id, object_id, segment_id, dmc_path)
        for query in queries:
            print("Generated Query:")
            print(query)
            
            # Validate query
            validation = validate_sql_query(query, conn)
            if validation["valid"]:
                print("Validation: Query is valid")
                result = execute_and_display_query(query, conn)
                print("Result:", result)
            else:
                print(f"Validation: {validation['message']}")
                if validation.get("fixed"):
                    print("Fixed Query:", validation["query"])
                    result = execute_and_display_query(validation["query"], conn)
                    print("Result:", result)
            
    except Exception as e:
        print(f"Error in main: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()